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
//! before settling leaves its entry stranded on the processing list. Recovering those (a ZSET
//! watchdog) is not implemented in 0.4; the durable, recoverable path is Redis Streams
//! ([`crate::RedisStream`]).
//!
//! Headers travel in a frame around the payload (see [`crate::envelope`]): a lossless binary frame
//! by default, or a readable codec-serialized envelope when a codec is set with
//! [`RedisList::codec`] / [`RedisListPublisher::codec`].

use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use fred::clients::Pool;
use fred::interfaces::{KeysInterface, ListInterface};
use fred::types::lists::LMoveDirection;
use futures::Stream;
use futures::stream::unfold;
use ruststream::codec::Codec;
use ruststream::runtime::RETRY_COUNT_HEADER;
use ruststream::{AckError, Headers, IncomingMessage, Partitioned, SubscriptionSource};

use crate::deadletter::{self, PoisonPolicy, REASON_DROPPED, REASON_MAX_DELIVERIES};
use crate::envelope::{SharedEnvelope, frame, unframe};
use crate::{RedisBroker, error::RedisError, message::PARTITION_KEY_HEADER};

const DEFAULT_BLOCK: Duration = Duration::from_secs(5);
/// Suffix appended to the list key to form the default per-consumer processing list (reliable mode).
const PROCESSING_SUFFIX: &str = ".processing";

fn block_secs(block: Duration) -> f64 {
    block.as_secs_f64()
}

/// Describes one list subscription against [`crate::RedisBroker`].
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
}

impl Debug for RedisList {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisList")
            .field("key", &self.key)
            .field("reliable", &self.reliable)
            .field("processing", &self.processing)
            .field("codec", &self.codec.is_some())
            .field("dead_letter", &self.dead_letter)
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
    /// on a simple list, which cannot ack. See [`crate::deadletter`].
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

    /// The list key this subscription consumes.
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
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
}

impl SubscriptionSource<RedisBroker> for RedisList {
    type Subscriber = RedisListSubscriber;

    fn name(&self) -> &str {
        self.key()
    }

    async fn subscribe(self, broker: &RedisBroker) -> Result<Self::Subscriber, RedisError> {
        broker.subscribe_list(self).await
    }
}

/// A list-backed work-queue subscription.
pub struct RedisListSubscriber {
    pool: Pool,
    key: String,
    reliable: bool,
    processing: String,
    block: Duration,
    codec: Option<SharedEnvelope>,
    policy: PoisonPolicy,
}

impl Debug for RedisListSubscriber {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisListSubscriber")
            .field("key", &self.key)
            .field("reliable", &self.reliable)
            .finish_non_exhaustive()
    }
}

impl RedisListSubscriber {
    pub(crate) fn new(
        pool: Pool,
        key: String,
        reliable: bool,
        processing: String,
        block: Duration,
        codec: Option<SharedEnvelope>,
        policy: PoisonPolicy,
    ) -> Self {
        Self {
            pool,
            key,
            reliable,
            processing,
            block,
            codec,
            policy,
        }
    }

    fn simple_message(&self, raw: &[u8]) -> RedisListMessage {
        let (payload, headers) = unframe(self.codec.as_ref(), raw);
        RedisListMessage {
            payload,
            headers,
            ack: None,
        }
    }

    fn reliable_message(&self, raw: Vec<u8>) -> RedisListMessage {
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
            }),
        }
    }

    /// Blocks for the next entry, returning `None` when the pop times out (the caller loops).
    async fn next_entry(&self) -> Result<Option<RedisListMessage>, RedisError> {
        let secs = block_secs(self.block);
        if self.reliable {
            let value: Option<Vec<u8>> = self
                .pool
                .blmove(
                    self.key.as_str(),
                    self.processing.as_str(),
                    LMoveDirection::Right,
                    LMoveDirection::Left,
                    secs,
                )
                .await
                .map_err(RedisError::stream)?;
            Ok(value.map(|v| self.reliable_message(v)))
        } else {
            let popped: Option<(String, Vec<u8>)> = self
                .pool
                .brpop(self.key.as_str(), secs)
                .await
                .map_err(RedisError::stream)?;
            Ok(popped.map(|(_, v)| self.simple_message(&v)))
        }
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
}

/// A list-queue delivery. In simple mode `ack` / `nack` are unsupported; in reliable mode `ack`
/// removes the entry from the processing list and `nack` either returns it or drops it.
pub struct RedisListMessage {
    payload: Bytes,
    headers: Headers,
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

    fn headers(&self) -> &Headers {
        &self.headers
    }

    async fn ack(self) -> Result<(), AckError> {
        let Some(handle) = self.ack else {
            return Err(AckError::Unsupported);
        };
        lrem(&handle).await
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
        lrem(&handle).await
    }
}

fn ack_broker(err: fred::error::Error) -> AckError {
    AckError::Broker(Box::new(err))
}

/// The next framework retry-count value (the current envelope header plus one, or one when absent).
fn next_retry_count(headers: &Headers) -> u64 {
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
    headers: &Headers,
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

async fn lrem(handle: &ListAck) -> Result<(), AckError> {
    let _: i64 = handle
        .pool
        .lrem(handle.processing_key.as_str(), 1, handle.value.clone())
        .await
        .map_err(ack_broker)?;
    Ok(())
}

impl Partitioned for RedisListMessage {
    fn partition_key(&self) -> Option<&[u8]> {
        self.headers().get(PARTITION_KEY_HEADER)
    }
}

/// Publishes onto a list with `LPUSH`, so right-popping consumers see FIFO order.
///
/// Obtain it from [`RedisBroker::list_publisher`](crate::RedisBroker::list_publisher). Headers are
/// framed around the payload; set a [`codec`](Self::codec) for a readable wire format (it must match
/// the subscriber's).
#[derive(Clone)]
pub struct RedisListPublisher {
    pool: Arc<tokio::sync::OnceCell<Pool>>,
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
    pub(crate) fn new(pool: Arc<tokio::sync::OnceCell<Pool>>) -> Self {
        Self {
            pool,
            codec: None,
            ttl: None,
        }
    }

    /// Serializes the header/payload envelope with `codec` (must match the subscriber). Without it
    /// the default lossless binary framing is used.
    #[must_use]
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
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::time::Duration;
    /// use ruststream_fred::RedisBroker;
    ///
    /// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = RedisBroker::connect("redis://localhost:6379").await?;
    /// let publisher = broker.list_publisher().ttl(Duration::from_secs(300));
    /// # let _ = publisher;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub const fn ttl(mut self, ttl: Duration) -> Self {
        self.ttl = Some(ttl);
        self
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
        let pool = self.pool.get().cloned().ok_or(RedisError::NotConnected)?;
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
