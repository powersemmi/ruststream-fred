//! Redis list transport: a competing-consumers work queue.
//!
//! A producer `LPUSH`es onto the list; consumers pop from the right (`BRPOP`), so delivery is FIFO
//! and each entry goes to exactly one consumer (no fan-out, no replay, no groups). Two modes:
//!
//! * Simple (default) - `BRPOP`, at-most-once. `ack` / `nack` report [`AckError::Unsupported`]: once
//!   popped, the entry is gone, so a crash mid-handler loses it.
//! * Reliable ([`RedisList::reliable`]) - `LMOVE` the entry to a per-consumer processing list, then
//!   `LREM` it on `ack` (at-least-once). `nack(requeue = true)` returns it to the main list;
//!   `nack(requeue = false)` removes it.
//!
//! Reliable mode has no native idle/pending tracking, so a consumer that dies after `LMOVE` but
//! before settling leaves its entry stranded on the processing list. Opting into a recovery ZSET
//! with [`RedisList::recovery_zset`] (and [`RedisList::min_idle`]) starts a watchdog that returns
//! such orphans to the main list; without it (the default) reliable lists have no orphan recovery,
//! and Redis Streams ([`crate::RedisStream`]) remain the recommended durable path.
//!
//! Headers travel in a frame around the payload: a lossless binary frame
//! by default, or a readable codec-serialized envelope when a codec is set with
//! [`RedisList::codec`] / [`RedisListPublish::codec`].

use std::fmt::{Debug, Formatter};
use std::future::{Future, ready};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use fred::clients::Pool;
use fred::error::ErrorKind;
use fred::interfaces::{KeysInterface, ListInterface};
use fred::types::lists::LMoveDirection;
use futures::Stream;
use futures::stream::unfold;
use ruststream::codec::Codec;
use ruststream::runtime::RETRY_COUNT_HEADER;
use ruststream::{
    AckError, BatchSubscriber, BufferedSubscriber, HeaderMap, IncomingMessage, PairError,
    Partitioned, PublishPolicy, SubscriptionSource,
};

use crate::broker::{ConnectedRedisBroker, RedisCore};
use crate::deadletter::{self, PoisonPolicy, REASON_DROPPED, REASON_MAX_DELIVERIES};
use crate::envelope::{SharedEnvelope, frame, unframe};
use crate::recovery::{self, RecoveryConfig};
use crate::{error::RedisError, message::PARTITION_KEY_HEADER};

/// This form's publish policy, [`RedisListPublish`], under the mount-site name every form gives
/// its own. Its options, the framing codec and the key TTL, still chain off it.
pub use crate::list::RedisListPublish as Publish;

/// The core prelude, the broker, the [`RedisList`] descriptor and this form's [`Publish`] policy.
/// A list carries no core capability traits.
///
/// # Examples
///
/// ```
/// use ruststream_fred::list::prelude::*;
///
/// let jobs = RedisList::new("jobs").reliable();
/// let broker = RedisBroker::standalone("redis://localhost:6379");
/// let replies: Publish = Publish::default();
/// let _ = (jobs, broker, replies);
/// ```
///
/// Two vocabularies that do not mix. A handler body imports `ruststream::prelude::*` and bounds an
/// injected slot with the broker capability trait it needs (`Out<impl Publisher>`); a routes file
/// globs this prelude and names the policy by its mount-site word, the same word on every form.
///
/// A file that also globs another form's prelude sees an ambiguous `Publish`; use
/// [`crate::prelude`] and write `list::Publish` there.
pub mod prelude {
    pub use ruststream::prelude::*;

    pub use super::{Publish, RedisList};
    pub use crate::{PARTITION_KEY_HEADER, RedisBroker, RedisPublishExt, RedisSubscribeExt};

    #[cfg(any(
        feature = "tls-rustls",
        feature = "tls-rustls-ring",
        feature = "tls-native-tls"
    ))]
    pub use crate::{TlsConfig, TlsConnector};
}

const DEFAULT_BLOCK: Duration = Duration::from_secs(5);
/// Suffix appended to the list key to form the default per-consumer processing list (reliable mode).
const PROCESSING_SUFFIX: &str = ".processing";

fn block_secs(block: Duration) -> f64 {
    block.as_secs_f64()
}

/// Normalizes a blocking pop (`BRPOP` / `BLMOVE`) result: fred reports a timed-out pop with nothing
/// available as a timeout error rather than an empty reply, so treat that as "no entry this round"
/// and let the read loop retry. Any other error propagates.
fn empty_on_timeout<T>(
    result: Result<Option<T>, fred::error::Error>,
) -> Result<Option<T>, RedisError> {
    match result {
        Ok(value) => Ok(value),
        Err(err) if matches!(err.kind(), ErrorKind::Timeout) => Ok(None),
        Err(err) => Err(RedisError::stream(err)),
    }
}

/// Describes one list subscription against a [`ConnectedRedisBroker`].
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use ruststream_fred::RedisList;
///
/// let simple = RedisList::new("jobs");
/// let reliable = RedisList::new("jobs").reliable().block(Duration::from_secs(2));
/// # let _ = (simple, reliable);
/// ```
#[derive(Clone)]
#[must_use]
pub struct RedisList {
    key: String,
    reliable: bool,
    processing: Option<String>,
    block: Option<Duration>,
    codec: Option<SharedEnvelope>,
    dead_letter: Option<String>,
    max_deliveries: Option<u64>,
    min_idle: Option<Duration>,
    recovery_zset: Option<String>,
    recovery_ttl: Option<Duration>,
}

impl Debug for RedisList {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisList")
            .field("key", &self.key)
            .field("reliable", &self.reliable)
            .field("processing", &self.processing)
            .field("codec", &self.codec.is_some())
            .field("dead_letter", &self.dead_letter)
            .field("max_deliveries", &self.max_deliveries)
            .field("recovery_zset", &self.recovery_zset)
            .field("recovery_ttl", &self.recovery_ttl)
            .finish_non_exhaustive()
    }
}

impl RedisList {
    /// A simple (at-most-once) `BRPOP` work-queue consumer on `key`.
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            reliable: false,
            processing: None,
            block: None,
            codec: None,
            dead_letter: None,
            max_deliveries: None,
            min_idle: None,
            recovery_zset: None,
            recovery_ttl: None,
        }
    }

    /// Switches to reliable (at-least-once) mode: entries move to a processing list and are removed
    /// on `ack`.
    pub const fn reliable(mut self) -> Self {
        self.reliable = true;
        self
    }

    /// Sets the processing-list key used in reliable mode. Defaults to `<key>.processing`.
    pub fn processing(mut self, key: impl Into<String>) -> Self {
        self.processing = Some(key.into());
        self
    }

    /// How long one blocking pop waits before looping. Defaults to 5 seconds.
    ///
    /// Also reachable at the mount site, after the page size, through
    /// [`RedisSubscribeExt`](crate::RedisSubscribeExt).
    pub const fn block(mut self, block: Duration) -> Self {
        self.block = Some(block);
        self
    }

    /// Decodes the header/payload envelope with `codec` (must match the publisher). Without it the
    /// default lossless binary framing is used.
    pub fn codec(mut self, codec: impl Codec + 'static) -> Self {
        self.codec = Some(Arc::new(codec));
        self
    }

    /// In reliable mode, routes dropped and poison entries to the named dead-letter list (`LPUSH`)
    /// instead of discarding them, tagged with
    /// [`DEAD_LETTER_REASON_HEADER`](crate::DEAD_LETTER_REASON_HEADER). Off by default. Has no effect
    /// on a simple list, which cannot ack. See the
    /// [dead-letter guide](https://powersemmi.github.io/ruststream-fred/latest/dead-letter/).
    pub fn dead_letter(mut self, key: impl Into<String>) -> Self {
        self.dead_letter = Some(key.into());
        self
    }

    /// In reliable mode, caps how many times an entry may be `nack(requeue = true)`-ed before it is
    /// treated as poison (dead-lettered or, with no dead-letter list, discarded). Off by default.
    ///
    /// Lists have no native delivery counter, so this tracks the framework retry-count header carried
    /// in the entry's envelope.
    pub const fn max_deliveries(mut self, max: u64) -> Self {
        self.max_deliveries = Some(max);
        self
    }

    /// How long a claimed reliable-mode entry may sit idle on the processing list before the
    /// recovery watchdog returns it to the main list. Required for (and only meaningful with)
    /// [`recovery_zset`](Self::recovery_zset).
    ///
    /// It has no default and must exceed the longest legitimate handler runtime: set it too low and
    /// a healthy consumer's in-flight entry gets recovered and processed twice.
    pub const fn min_idle(mut self, min_idle: Duration) -> Self {
        self.min_idle = Some(min_idle);
        self
    }

    /// Opts reliable mode into orphan recovery, naming the ZSET key that tracks in-flight claims.
    ///
    /// Off by default (a dead consumer's entry stays stranded on the processing list). The key has
    /// no sane default, so it is named explicitly here; pair it with [`min_idle`](Self::min_idle),
    /// which is required when recovery is on. Reliable mode is implied. See
    /// [orphan recovery](https://powersemmi.github.io/ruststream-fred/latest/lists/#orphan-recovery).
    pub fn recovery_zset(mut self, key: impl Into<String>) -> Self {
        self.recovery_zset = Some(key.into());
        self.reliable = true;
        self
    }

    /// An optional auto-cleanup TTL on the recovery ZSET key (refreshed on every claim).
    ///
    /// When set it must exceed [`min_idle`](Self::min_idle) (and the longest legitimate handler
    /// runtime), or in-flight tracking is dropped before the watchdog can act.
    pub const fn recovery_ttl(mut self, ttl: Duration) -> Self {
        self.recovery_ttl = Some(ttl);
        self
    }

    /// The list key this subscription consumes.
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Consumes the definition for its key, so opening a subscription moves the string out
    /// instead of copying it.
    pub(crate) fn into_key(self) -> String {
        self.key
    }

    pub(crate) const fn is_reliable(&self) -> bool {
        self.reliable
    }

    pub(crate) fn processing_or_default(&self) -> String {
        self.processing
            .clone()
            .unwrap_or_else(|| format!("{}{PROCESSING_SUFFIX}", self.key))
    }

    pub(crate) fn block_or_default(&self) -> Duration {
        self.block.unwrap_or(DEFAULT_BLOCK)
    }

    pub(crate) fn codec_handle(&self) -> Option<SharedEnvelope> {
        self.codec.clone()
    }

    pub(crate) fn poison_policy(&self) -> PoisonPolicy {
        PoisonPolicy {
            dead_letter: self.dead_letter.clone(),
            max_deliveries: self.max_deliveries,
        }
    }

    /// Resolves the recovery settings, or `None` when recovery was not opted into.
    ///
    /// # Errors
    ///
    /// Returns [`RedisError::InvalidOptions`] when a recovery ZSET is named without a
    /// [`min_idle`](Self::min_idle), which has no sane default.
    pub(crate) fn recovery_config(&self) -> Result<Option<RecoveryConfig>, RedisError> {
        let Some(zset_key) = self.recovery_zset.clone() else {
            return Ok(None);
        };
        let min_idle = self.min_idle.ok_or_else(|| {
            RedisError::InvalidOptions(format!(
                "reliable list recovery on `{}` needs a min_idle: call .min_idle(duration) \
                 alongside .recovery_zset(key)",
                self.key
            ))
        })?;
        Ok(Some(RecoveryConfig {
            zset_key,
            min_idle,
            ttl: self.recovery_ttl,
        }))
    }
}

impl SubscriptionSource<ConnectedRedisBroker> for RedisList {
    type Subscriber = RedisListSubscriber;

    fn name(&self) -> &str {
        self.key()
    }

    async fn subscribe(
        self,
        connected: &ConnectedRedisBroker,
    ) -> Result<Self::Subscriber, RedisError> {
        connected.subscribe_list(self).await
    }
}

#[cfg(feature = "testing")]
impl SubscriptionSource<crate::testing::ConnectedRedisTestBroker> for RedisList {
    type Subscriber = crate::testing::RedisTestSubscriber;

    fn name(&self) -> &str {
        self.key()
    }

    async fn subscribe(
        self,
        connected: &crate::testing::ConnectedRedisTestBroker,
    ) -> Result<Self::Subscriber, RedisError> {
        connected.subscribe(self.key()).await
    }
}

/// A list-backed work-queue subscription.
///
/// A pop returns one entry, so the pages a [`BatchSubscriber`] hands out are assembled on the
/// client by the core's [`BufferedSubscriber`]: it fills a page up to the size the mount site
/// asked for and closes a partial one on its own deadline.
pub struct RedisListSubscriber(BufferedSubscriber<ListWire>);

impl Debug for RedisListSubscriber {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("RedisListSubscriber").field(&self.0).finish()
    }
}

impl RedisListSubscriber {
    #[allow(
        clippy::too_many_arguments,
        reason = "internal constructor mirroring the descriptor"
    )]
    pub(crate) fn new(
        pool: Pool,
        key: String,
        reliable: bool,
        processing: String,
        block: Duration,
        codec: Option<SharedEnvelope>,
        policy: PoisonPolicy,
        recovery: Option<RecoveryConfig>,
    ) -> Self {
        Self(BufferedSubscriber::new(ListWire {
            pool,
            key,
            reliable,
            processing,
            block,
            codec,
            policy,
            recovery,
        }))
    }
}

impl ruststream::Subscriber for RedisListSubscriber {
    type Message = RedisListMessage;
    type Error = RedisError;

    /// Yields one message per popped entry.
    ///
    /// # Cancel safety
    ///
    /// Dropping the returned stream between items is safe. In reliable mode an entry already moved
    /// to the processing list but not yet settled stays there until acked or recovered manually.
    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        self.0.stream()
    }
}

impl BatchSubscriber for RedisListSubscriber {
    type Batch = Vec<RedisListMessage>;

    /// Yields pages of at most `size` entries, assembled from consecutive pops.
    ///
    /// # Cancel safety
    ///
    /// Same as [`Subscriber::stream`](ruststream::Subscriber::stream). Dropping the stream while a
    /// page is filling abandons the entries it holds unsettled; in reliable mode they stay on the
    /// processing list until recovered.
    fn batches(
        &mut self,
        size: NonZeroUsize,
    ) -> impl Stream<Item = Result<Self::Batch, Self::Error>> + Send + '_ {
        self.0.batches(size)
    }
}

/// The wire side of a list subscription: one blocking pop per delivery.
struct ListWire {
    pool: Pool,
    key: String,
    reliable: bool,
    processing: String,
    block: Duration,
    codec: Option<SharedEnvelope>,
    policy: PoisonPolicy,
    recovery: Option<RecoveryConfig>,
}

impl Debug for ListWire {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ListWire")
            .field("key", &self.key)
            .field("reliable", &self.reliable)
            .field("poison", &self.policy.is_active())
            .field("recovery", &self.recovery.is_some())
            .finish_non_exhaustive()
    }
}

impl ListWire {
    fn simple_message(&self, raw: &[u8]) -> RedisListMessage {
        let (payload, headers) = unframe(self.codec.as_ref(), raw);
        RedisListMessage {
            payload,
            headers,
            ack: None,
        }
    }

    fn reliable_message(&self, raw: Vec<u8>, recovery: Option<RecoveryHandle>) -> RedisListMessage {
        let (payload, headers) = unframe(self.codec.as_ref(), &raw);
        RedisListMessage {
            payload,
            headers,
            ack: Some(ListAck {
                pool: self.pool.clone(),
                main_key: self.key.clone(),
                processing_key: self.processing.clone(),
                value: raw,
                codec: self.codec.clone(),
                policy: self.policy.clone(),
                recovery,
            }),
        }
    }

    /// Blocks for the next entry, returning `None` when the pop times out (the caller loops). When
    /// recovery is enabled, first returns any orphaned entries to the main list so this same pop can
    /// pick them up.
    async fn next_entry(&self) -> Result<Option<RedisListMessage>, RedisError> {
        let secs = block_secs(self.block);
        if self.reliable {
            if let Some(cfg) = &self.recovery {
                recovery::sweep_orphans(&self.pool, cfg, &self.key, &self.processing).await?;
            }
            let value: Option<Vec<u8>> = empty_on_timeout(
                self.pool
                    .blmove(
                        self.key.as_str(),
                        self.processing.as_str(),
                        LMoveDirection::Right,
                        LMoveDirection::Left,
                        secs,
                    )
                    .await,
            )?;
            let Some(value) = value else {
                return Ok(None);
            };
            let handle = match &self.recovery {
                Some(cfg) => {
                    let member = recovery::record_claim(&self.pool, cfg, &value).await?;
                    Some(RecoveryHandle {
                        zset_key: cfg.zset_key.clone(),
                        member,
                    })
                }
                None => None,
            };
            Ok(Some(self.reliable_message(value, handle)))
        } else {
            let popped: Option<(String, Vec<u8>)> =
                empty_on_timeout(self.pool.brpop(self.key.as_str(), secs).await)?;
            Ok(popped.map(|(_, v)| self.simple_message(&v)))
        }
    }
}

impl ruststream::Subscriber for ListWire {
    type Message = RedisListMessage;
    type Error = RedisError;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        unfold(&*self, |s| async move {
            loop {
                match s.next_entry().await {
                    Ok(Some(msg)) => return Some((Ok(msg), s)),
                    Ok(None) => {}
                    Err(err) => return Some((Err(err), s)),
                }
            }
        })
    }
}

/// Settlement handle for a reliable-mode list delivery.
struct ListAck {
    pool: Pool,
    main_key: String,
    processing_key: String,
    /// The raw wire value (framed), needed verbatim to `LREM` it from the processing list.
    value: Vec<u8>,
    /// The framing codec, so a poison-policy requeue can re-frame with an updated retry count.
    codec: Option<SharedEnvelope>,
    policy: PoisonPolicy,
    /// Set when orphan recovery is enabled: the ZSET key and the member tracking this claim, so
    /// settling removes its recovery tracking.
    recovery: Option<RecoveryHandle>,
}

/// The recovery-ZSET coordinates for one in-flight reliable-list claim.
struct RecoveryHandle {
    zset_key: String,
    member: Vec<u8>,
}

/// A list-queue delivery. In simple mode `ack` / `nack` are unsupported; in reliable mode `ack`
/// removes the entry from the processing list and `nack` either returns it or drops it.
pub struct RedisListMessage {
    payload: Bytes,
    headers: HeaderMap,
    ack: Option<ListAck>,
}

impl Debug for RedisListMessage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisListMessage")
            .field("payload_len", &self.payload.len())
            .field("reliable", &self.ack.is_some())
            .finish_non_exhaustive()
    }
}

impl IncomingMessage for RedisListMessage {
    fn payload(&self) -> &[u8] {
        &self.payload
    }

    fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    // See `RedisMessage`: the keyed worker lanes read the key from here, not from `Partitioned`.
    fn partition_key(&self) -> Option<&[u8]> {
        Partitioned::partition_key(self)
    }

    async fn ack(self) -> Result<(), AckError> {
        let Some(handle) = self.ack else {
            return Err(AckError::Unsupported);
        };
        settle(&handle).await
    }

    async fn nack(self, requeue: bool) -> Result<(), AckError> {
        let Some(handle) = self.ack else {
            return Err(AckError::Unsupported);
        };
        if requeue {
            if handle.policy.is_active() {
                let next = next_retry_count(&self.headers);
                if handle.policy.is_poison(next) {
                    list_dead_letter(&handle, &self.payload, &self.headers, REASON_MAX_DELIVERIES)
                        .await?;
                } else {
                    // Re-frame with the incremented retry count and return it to the main list,
                    // before removing the original from processing (a crash leaves a duplicate).
                    let mut headers = self.headers.clone();
                    headers.insert(RETRY_COUNT_HEADER, next.to_string());
                    let body = frame(handle.codec.as_ref(), &self.payload, &headers);
                    lpush(&handle.pool, handle.main_key.as_str(), body).await?;
                }
            } else {
                // No poison policy: return the original entry verbatim to the main list.
                lpush(&handle.pool, handle.main_key.as_str(), handle.value.clone()).await?;
            }
        } else if handle.policy.is_active() {
            list_dead_letter(&handle, &self.payload, &self.headers, REASON_DROPPED).await?;
        }
        settle(&handle).await
    }
}

fn ack_broker(err: fred::error::Error) -> AckError {
    AckError::Broker(Box::new(err))
}

/// The next framework retry-count value (the current envelope header plus one, or one when absent).
fn next_retry_count(headers: &HeaderMap) -> u64 {
    headers
        .get_str(RETRY_COUNT_HEADER)
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0)
        + 1
}

async fn lpush(pool: &Pool, key: &str, body: Vec<u8>) -> Result<(), AckError> {
    let _: i64 = pool.lpush(key, body).await.map_err(ack_broker)?;
    Ok(())
}

/// `LPUSH`es a tagged copy onto the configured dead-letter list, or does nothing when none is set
/// (the caller's `LREM` then discards the entry). Runs before the `LREM`, so a crash leaves a
/// duplicate rather than a loss.
async fn list_dead_letter(
    handle: &ListAck,
    payload: &[u8],
    headers: &HeaderMap,
    reason: &'static str,
) -> Result<(), AckError> {
    if let Some(dlq) = handle.policy.dead_letter_key() {
        let body = frame(
            handle.codec.as_ref(),
            payload,
            &deadletter::with_reason(headers, reason),
        );
        lpush(&handle.pool, dlq, body).await?;
    }
    Ok(())
}

/// Removes the entry from the processing list and, when recovery is enabled, drops its tracking from
/// the recovery ZSET.
async fn settle(handle: &ListAck) -> Result<(), AckError> {
    let _: i64 = handle
        .pool
        .lrem(handle.processing_key.as_str(), 1, handle.value.clone())
        .await
        .map_err(ack_broker)?;
    if let Some(rec) = &handle.recovery {
        recovery::forget(&handle.pool, &rec.zset_key, &rec.member).await?;
    }
    Ok(())
}

impl Partitioned for RedisListMessage {
    fn partition_key(&self) -> Option<&[u8]> {
        self.headers().get(PARTITION_KEY_HEADER)
    }
}

/// The declaration half of the list publisher: envelope codec and key TTL, no connection.
///
/// Constructible anywhere, it pairs into a [`RedisListPublisher`] against a
/// [`ConnectedRedisBroker`].
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use ruststream::codec::JsonCodec;
/// use ruststream_fred::RedisListPublish;
///
/// let publish = RedisListPublish::new()
///     .codec(JsonCodec)
///     .ttl(Duration::from_secs(300));
/// # let _ = publish;
/// ```
#[derive(Clone, Default)]
#[must_use]
pub struct RedisListPublish {
    codec: Option<SharedEnvelope>,
    ttl: Option<Duration>,
}

impl Debug for RedisListPublish {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisListPublish")
            .field("codec", &self.codec.is_some())
            .field("ttl", &self.ttl)
            .finish()
    }
}

impl RedisListPublish {
    /// A policy with the default binary framing and no key TTL. Equivalent to [`Self::default`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Serializes the header/payload envelope with `codec` (must match the subscriber). Without it
    /// the default lossless binary framing is used.
    pub fn codec(mut self, codec: impl Codec + 'static) -> Self {
        self.codec = Some(Arc::new(codec));
        self
    }

    /// Sets a time-to-live on the list key, refreshed (`PEXPIRE`) on every publish, so an idle
    /// queue auto-expires. Off by default: without it the list lives until drained or deleted.
    ///
    /// This is a per-key TTL on the whole list, not per-entry: Redis lists have no per-element
    /// expiry, only the key can expire. Each publish pushes the entry and re-arms the key's TTL in
    /// one pipeline, so an actively used queue never expires and only an idle one does. A sub-
    /// millisecond `ttl` is clamped up to 1ms, since `PEXPIRE 0` would delete the key outright.
    pub const fn ttl(mut self, ttl: Duration) -> Self {
        self.ttl = Some(ttl);
        self
    }
}

impl PublishPolicy<ConnectedRedisBroker> for RedisListPublish {
    type Live = RedisListPublisher;

    fn pair(
        self,
        connected: &ConnectedRedisBroker,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(connected.list_publisher(self)))
    }
}

/// Publishes onto a list with `LPUSH`, so right-popping consumers see FIFO order: a
/// [`RedisListPublish`] policy paired with a connection.
///
/// Obtain it from
/// [`ConnectedRedisBroker::list_publisher`](crate::ConnectedRedisBroker::list_publisher), or by
/// pairing the policy. Publishing after the connection was shut down reports
/// [`RedisError::ShutDown`].
#[derive(Clone)]
pub struct RedisListPublisher {
    core: Arc<RedisCore>,
    codec: Option<SharedEnvelope>,
    ttl: Option<Duration>,
}

impl Debug for RedisListPublisher {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisListPublisher")
            .field("codec", &self.codec.is_some())
            .field("ttl", &self.ttl)
            .finish_non_exhaustive()
    }
}

impl RedisListPublisher {
    pub(crate) fn new(core: Arc<RedisCore>, publish: RedisListPublish) -> Self {
        Self {
            core,
            codec: publish.codec,
            ttl: publish.ttl,
        }
    }
}

/// Converts a TTL to the positive millisecond count `PEXPIRE` expects, clamping a sub-millisecond
/// value up to 1 (a `PEXPIRE 0` deletes the key instead of expiring it).
fn ttl_millis(ttl: Duration) -> i64 {
    i64::try_from(ttl.as_millis()).unwrap_or(i64::MAX).max(1)
}

impl ruststream::Publisher for RedisListPublisher {
    type Error = RedisError;

    async fn publish(&self, msg: ruststream::OutgoingMessage<'_>) -> Result<(), Self::Error> {
        let pool = self.core.pool()?;
        let body = frame(self.codec.as_ref(), msg.payload(), msg.headers());
        let Some(ttl) = self.ttl else {
            let _: i64 = pool
                .lpush(msg.name(), body)
                .await
                .map_err(RedisError::publish)?;
            return Ok(());
        };
        // Push the entry and re-arm the key TTL in one pipeline, so an actively used queue keeps
        // resetting its expiry and only an idle one is allowed to lapse.
        let pipeline = pool.next().pipeline();
        let _: () = pipeline
            .lpush(msg.name(), body)
            .await
            .map_err(RedisError::publish)?;
        let _: () = pipeline
            .pexpire(msg.name(), ttl_millis(ttl), None)
            .await
            .map_err(RedisError::publish)?;
        let _: Vec<fred::types::Value> = pipeline.all().await.map_err(RedisError::publish)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ttl_millis_converts_and_clamps() {
        assert_eq!(ttl_millis(Duration::from_secs(60)), 60_000);
        assert_eq!(ttl_millis(Duration::from_millis(1)), 1);
        // A sub-millisecond TTL must not become PEXPIRE 0 (which deletes the key).
        assert_eq!(ttl_millis(Duration::from_nanos(1)), 1);
        assert_eq!(ttl_millis(Duration::ZERO), 1);
    }
}
