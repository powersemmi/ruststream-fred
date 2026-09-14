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
//! Headers travel in a frame around the payload: a binary frame by default, or a readable
//! codec-serialized envelope when a codec is set with [`RedisList::codec`] /
//! [`RedisListPublish::codec`]. Both framings are lossless; the envelope writes a field whose
//! bytes are valid UTF-8 as text and any other bytes as themselves.

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
#[cfg(feature = "asyncapi")]
use ruststream::asyncapi::Bindings;
use ruststream::codec::Codec;
use ruststream::{
    AckError, AddressedCopies, BatchSubscriber, BufferedSubscriber, HeaderMap, IncomingMessage,
    PairError, Partitioned, PublishPolicy, RedeliveryAddress, RedeliveryAddressed,
    SubscriptionSource,
};

use crate::broker::{ConnectedRedisBroker, RedisCore};
use crate::envelope::{SharedEnvelope, frame, unframe};
use crate::partition::{RedisPublishOptions, resolved_headers};
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
    pub use crate::{
        PARTITION_KEY_HEADER, RedisBroker, RedisPublishOptions, RedisPublishSteps,
        RedisSubscribeExt,
    };

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
    /// Also reachable at the mount site, after the batch size, through
    /// [`RedisSubscribeExt`](crate::RedisSubscribeExt).
    pub const fn block(mut self, block: Duration) -> Self {
        self.block = Some(block);
        self
    }

    /// Decodes the header/payload envelope with `codec` (must match the publisher). Without it the
    /// default binary framing is used. Either way the payload arrives as it was published.
    pub fn codec(mut self, codec: impl Codec + 'static) -> Self {
        self.codec = Some(Arc::new(codec));
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

    /// What this subscription adds to its channel in the generated `AsyncAPI` document, shared by
    /// both broker forms: whether it acknowledges, the processing list it holds claims on, and
    /// how headers are framed beside the payload.
    #[cfg(feature = "asyncapi")]
    fn describe(&self) -> Bindings {
        let processing = self.reliable.then(|| self.processing_or_default());
        crate::asyncapi::channel(&crate::asyncapi::ListSubscription::new(
            self.reliable,
            processing.as_deref(),
            crate::asyncapi::Envelope::of(self.codec.as_ref()),
        ))
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
    /// A retry copy is an `LPUSH` this process makes, and the list key is where it goes.
    type Copies = AddressedCopies;

    fn name(&self) -> &str {
        self.key()
    }

    async fn subscribe(
        self,
        connected: &ConnectedRedisBroker,
    ) -> Result<Self::Subscriber, RedisError> {
        connected.subscribe_list(self).await
    }

    #[cfg(feature = "asyncapi")]
    fn channel_bindings(&self) -> Bindings {
        self.describe()
    }
}

/// The list key: the subscription pops from it and [`RedisListPublish`] pushes onto it, so a
/// deferred copy lands in the same queue the delivery came from.
impl RedeliveryAddressed<ConnectedRedisBroker> for RedisList {
    fn redelivery_address(
        &self,
        _connected: &ConnectedRedisBroker,
    ) -> impl Future<Output = Result<RedeliveryAddress, RedisError>> {
        ready(Ok(RedeliveryAddress::new(self.key.clone())))
    }
}

/// Mounts the production descriptor on the in-process stand-in, which routes by list key alone.
///
/// The descriptor is validated exactly as [`ConnectedRedisBroker::subscribe_list`] validates it,
/// so a subscription that a real server would refuse at startup is refused here too rather than
/// passing a test and failing on deployment: a recovery ZSET named without a
/// [`min_idle`](RedisList::min_idle) is rejected.
///
/// The rest of the descriptor is inert here, because the stand-in has one queue per key and
/// delivers on publish: the processing list behind [`reliable`](RedisList::reliable), `block`,
/// and the orphan-recovery watchdog. The envelope
/// [`codec`](RedisList::codec) is inert too, since deliveries carry their headers natively instead
/// of framed into the entry, so a framing mismatch between a subscription and its publisher cannot
/// surface in process.
///
/// What `reliable` does decide is settlement, and that matches the real transport: a reliable list
/// acknowledges, while a simple one reports [`AckError::Unsupported`] here exactly as it does
/// against a real server, so a test cannot assert on an acknowledgement the mode cannot make.
#[cfg(feature = "testing")]
impl SubscriptionSource<crate::testing::ConnectedRedisTestBroker> for RedisList {
    type Subscriber = crate::testing::RedisTestSubscriber;
    /// The answer the real broker gives, so a registration that compiles against Redis compiles
    /// against the stand-in.
    type Copies = AddressedCopies;

    fn name(&self) -> &str {
        self.key()
    }

    async fn subscribe(
        self,
        connected: &crate::testing::ConnectedRedisTestBroker,
    ) -> Result<Self::Subscriber, RedisError> {
        self.recovery_config()?;
        if self.is_reliable() {
            connected.subscribe(self.key()).await
        } else {
            connected.subscribe_unsettleable(self.key()).await
        }
    }

    /// The same body the real broker's descriptor writes, so a document built in a test is the
    /// document the service publishes.
    #[cfg(feature = "asyncapi")]
    fn channel_bindings(&self) -> Bindings {
        self.describe()
    }
}

/// The same answer the real broker gives, so a scope that starts against Redis starts here.
#[cfg(feature = "testing")]
impl RedeliveryAddressed<crate::testing::ConnectedRedisTestBroker> for RedisList {
    fn redelivery_address(
        &self,
        _connected: &crate::testing::ConnectedRedisTestBroker,
    ) -> impl Future<Output = Result<RedeliveryAddress, RedisError>> {
        ready(Ok(RedeliveryAddress::new(self.key.clone())))
    }
}

/// A list-backed work-queue subscription.
///
/// A pop returns one entry, so the batches a [`BatchSubscriber`] hands out are assembled on the
/// client by the core's [`BufferedSubscriber`]: it fills a batch up to the size the mount site
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
        recovery: Option<RecoveryConfig>,
    ) -> Self {
        Self(BufferedSubscriber::new(ListWire {
            pool,
            key,
            reliable,
            processing,
            block,
            codec,
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

    /// Yields batches of at most `size` entries, assembled from consecutive pops.
    ///
    /// # Cancel safety
    ///
    /// Same as [`Subscriber::stream`](ruststream::Subscriber::stream). Dropping the stream while a
    /// batch is filling abandons the entries it holds unsettled; in reliable mode they stay on the
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
    recovery: Option<RecoveryConfig>,
}

impl Debug for ListWire {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ListWire")
            .field("key", &self.key)
            .field("reliable", &self.reliable)
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
            // Return the original entry verbatim to the main list, before removing it from
            // processing (a crash in between leaves a duplicate rather than a loss).
            lpush(&handle.pool, handle.main_key.as_str(), handle.value.clone()).await?;
        }
        settle(&handle).await
    }
}

fn ack_broker(err: fred::error::Error) -> AckError {
    AckError::Broker(Box::new(err))
}

async fn lpush(pool: &Pool, key: &str, body: Vec<u8>) -> Result<(), AckError> {
    let _: i64 = pool.lpush(key, body).await.map_err(ack_broker)?;
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

    /// Serializes the header/payload envelope with `codec` (must match the subscriber), which makes
    /// the wire value readable while the data is text. Without it the default binary framing is
    /// used. Either way a payload that is not text is published byte for byte.
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

    /// The list key the policy pushes onto, the expiry it re-arms there on every push, and how
    /// headers are framed beside the payload.
    #[cfg(feature = "asyncapi")]
    fn channel_bindings(&self, channel: &str) -> Bindings {
        crate::asyncapi::channel(&crate::asyncapi::Publish::list(
            channel,
            self.ttl
                .map(|ttl| u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX)),
            crate::asyncapi::Envelope::of(self.codec.as_ref()),
        ))
    }
}

/// Pairs the production policy against the in-process stand-in, so a routes file's
/// `.out_reply(Publish)` mounts on both without naming a second type.
///
/// Both options the policy carries are inert in process: the stand-in has no key to expire, so
/// [`ttl`](RedisListPublish::ttl) has nothing to re-arm, and it delivers headers natively rather
/// than framed into the entry, so the envelope [`codec`](RedisListPublish::codec) never runs.
/// A published entry therefore reads back as the bare payload here and as a frame on a real
/// server, which is what a `published(..)` assertion sees.
///
/// The capability surface matches: this pairs into
/// [`RedisTestPlainPublisher`](crate::testing::RedisTestPlainPublisher), which offers
/// [`Publisher`](ruststream::Publisher) and nothing more, exactly as [`RedisListPublisher`] does.
/// A slot bounded on a transaction capability therefore fails to compile here too, rather than
/// passing in process and breaking on the production build.
#[cfg(feature = "testing")]
impl PublishPolicy<crate::testing::ConnectedRedisTestBroker> for RedisListPublish {
    type Live = crate::testing::RedisTestPlainPublisher;

    fn pair(
        self,
        connected: &crate::testing::ConnectedRedisTestBroker,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(connected.plain_publisher()))
    }

    /// The list key the policy pushes onto, the expiry it re-arms there on every push, and how
    /// headers are framed beside the payload.
    #[cfg(feature = "asyncapi")]
    fn channel_bindings(&self, channel: &str) -> Bindings {
        crate::asyncapi::channel(&crate::asyncapi::Publish::list(
            channel,
            self.ttl
                .map(|ttl| u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX)),
            crate::asyncapi::Envelope::of(self.codec.as_ref()),
        ))
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
    /// `LPUSH` carries the key and the value and nothing else; the key TTL is a property of the
    /// queue, fixed by the policy and re-armed on every publish. What a call site still says is
    /// the partition key, framed into the entry with the message's other headers.
    type Options = RedisPublishOptions;

    async fn publish(
        &self,
        msg: ruststream::OutgoingMessage<'_>,
        options: Option<&Self::Options>,
    ) -> Result<(), Self::Error> {
        let pool = self.core.pool()?;
        let body = frame(
            self.codec.as_ref(),
            msg.payload(),
            &resolved_headers(msg.headers(), options),
        );
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
