//! Builder describing one Redis Streams subscription.
//!
//! A subscription always reads through a consumer group. Two read modes are selected by
//! constructor, never by a runtime flag, because they return disjoint message sets:
//!
//! * [`RedisStream::new`] reads fresh entries off the tail (`XREADGROUP > ...`).
//! * [`RedisStream::reclaim`] reads stale pending entries another consumer never acked
//!   (`XAUTOCLAIM`, idle at least `min_idle`) - the crash-recovery path.
//!
//! Inferring the mode from a numeric parameter would be a footgun (a stray idle timeout could
//! silently stop fresh delivery), so the mode is part of the constructor name. Recovery is a
//! separate `reclaim` subscriber on the same group: "two handlers per group".

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use ruststream::SubscriptionSource;

use crate::deadletter::PoisonPolicy;
use crate::{RedisBroker, error::RedisError, subscriber::RedisSubscriber};

const DEFAULT_COUNT: u64 = 64;
const DEFAULT_BLOCK: Duration = Duration::from_secs(5);

/// Generates an automatic consumer name when the caller does not set one. Distinct names keep
/// each in-process subscriber's pending list separate within a shared group.
fn auto_consumer() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("ruststream-{n}")
}

/// Where a freshly created consumer group starts reading from. Only consulted when the group does
/// not yet exist; an existing group keeps its own cursor.
#[derive(Debug, Clone, Default)]
pub enum StreamStart {
    /// Only entries added after the group is created (`$`). The default.
    #[default]
    New,
    /// Every entry currently in the stream (`0`).
    Beginning,
    /// A specific entry ID, exclusive.
    Id(String),
}

impl StreamStart {
    pub(crate) fn as_id(&self) -> &str {
        match self {
            Self::New => "$",
            Self::Beginning => "0",
            Self::Id(id) => id,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum ReadMode {
    /// `XREADGROUP >` - fresh tail.
    Fresh,
    /// `XAUTOCLAIM` of entries idle at least this long.
    Reclaim { min_idle: Duration },
}

/// Describes one Redis Streams subscription against [`crate::RedisBroker`].
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use ruststream_fred::RedisStream;
///
/// // Fresh tail: a normal worker reading new entries.
/// let fresh = RedisStream::new("orders").group("workers").count(128);
///
/// // Recovery: reclaim entries a crashed worker left pending for over 30s.
/// let recover = RedisStream::reclaim("orders", Duration::from_secs(30)).group("workers");
/// # let _ = (fresh, recover);
/// ```
#[derive(Debug, Clone)]
#[must_use]
pub struct RedisStream {
    key: String,
    group: Option<String>,
    consumer: Option<String>,
    count: Option<u64>,
    block: Option<Duration>,
    start: StreamStart,
    mode: ReadMode,
    dead_letter: Option<String>,
    max_deliveries: Option<u64>,
}

impl RedisStream {
    /// A fresh-tail subscription on `key`: reads new entries via `XREADGROUP >`.
    ///
    /// A consumer group is required; set it with [`group`](Self::group).
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            group: None,
            consumer: None,
            count: None,
            block: None,
            start: StreamStart::New,
            mode: ReadMode::Fresh,
            dead_letter: None,
            max_deliveries: None,
        }
    }

    /// A recovery subscription on `key`: reclaims pending entries idle at least `min_idle` via
    /// `XAUTOCLAIM`. Run it alongside a [`new`](Self::new) subscriber on the same group to pick up
    /// messages a consumer fetched but died before acking.
    ///
    /// `min_idle` has no default and must exceed the longest legitimate handler runtime: set it too
    /// low and a healthy consumer's in-flight message gets reclaimed and processed twice.
    pub fn reclaim(key: impl Into<String>, min_idle: Duration) -> Self {
        Self {
            key: key.into(),
            group: None,
            consumer: None,
            count: None,
            block: None,
            start: StreamStart::New,
            mode: ReadMode::Reclaim { min_idle },
            dead_letter: None,
            max_deliveries: None,
        }
    }

    /// Sets the consumer group. Required for every subscription.
    pub fn group(mut self, group: impl Into<String>) -> Self {
        self.group = Some(group.into());
        self
    }

    /// Sets this consumer's name within the group. Defaults to an auto-generated unique name.
    pub fn consumer(mut self, consumer: impl Into<String>) -> Self {
        self.consumer = Some(consumer.into());
        self
    }

    /// Upper bound on entries fetched per read. Defaults to 64.
    pub const fn count(mut self, count: u64) -> Self {
        self.count = Some(count);
        self
    }

    /// How long one read blocks waiting for entries. Defaults to 5 seconds. In fresh-tail mode this
    /// is the `XREADGROUP` server-side block; in reclaim mode `XAUTOCLAIM` does not block, so this is
    /// the poll interval slept between scans that find nothing to reclaim.
    pub const fn block(mut self, block: Duration) -> Self {
        self.block = Some(block);
        self
    }

    /// Where a newly created group starts reading. Ignored if the group already exists. Only
    /// meaningful for the fresh-tail [`new`](Self::new) mode.
    pub fn start_id(mut self, start: StreamStart) -> Self {
        self.start = start;
        self
    }

    /// Routes dropped and poison messages to the named dead-letter stream instead of discarding
    /// them. Off by default. The copy is tagged with
    /// [`DEAD_LETTER_REASON_HEADER`](crate::DEAD_LETTER_REASON_HEADER). See [`crate::deadletter`].
    pub fn dead_letter(mut self, key: impl Into<String>) -> Self {
        self.dead_letter = Some(key.into());
        self
    }

    /// Caps how many times a message may be delivered before it is treated as poison (dead-lettered
    /// or, with no dead-letter stream, discarded). Off by default.
    ///
    /// The cap is checked against both the framework retry-count header (the `nack`/republish loop)
    /// and the native stream delivery count (the reclaim loop), so a message poisoning either way is
    /// caught.
    pub const fn max_deliveries(mut self, max: u64) -> Self {
        self.max_deliveries = Some(max);
        self
    }

    /// The stream key this subscription reads.
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    pub(crate) fn group_or_err(&self) -> Result<&str, RedisError> {
        self.group.as_deref().ok_or_else(|| {
            RedisError::InvalidOptions(format!(
                "stream subscription on `{}` requires a consumer group: call .group(name)",
                self.key
            ))
        })
    }

    pub(crate) fn consumer_or_auto(&self) -> String {
        self.consumer.clone().unwrap_or_else(auto_consumer)
    }

    pub(crate) fn count_or_default(&self) -> u64 {
        self.count.unwrap_or(DEFAULT_COUNT)
    }

    pub(crate) fn block_or_default(&self) -> Duration {
        self.block.unwrap_or(DEFAULT_BLOCK)
    }

    pub(crate) const fn start(&self) -> &StreamStart {
        &self.start
    }

    pub(crate) fn mode(&self) -> ReadMode {
        self.mode.clone()
    }

    pub(crate) fn poison_policy(&self) -> PoisonPolicy {
        PoisonPolicy {
            dead_letter: self.dead_letter.clone(),
            max_deliveries: self.max_deliveries,
        }
    }
}

impl SubscriptionSource<RedisBroker> for RedisStream {
    type Subscriber = RedisSubscriber;

    fn name(&self) -> &str {
        self.key()
    }

    async fn subscribe(self, broker: &RedisBroker) -> Result<Self::Subscriber, RedisError> {
        broker.subscribe(self).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn group_is_required() {
        let err = RedisStream::new("orders").group_or_err().unwrap_err();
        assert!(matches!(err, RedisError::InvalidOptions(msg) if msg.contains("consumer group")));
    }

    #[test]
    fn group_set_resolves() {
        let s = RedisStream::new("orders").group("workers");
        assert_eq!(s.group_or_err().expect("group set"), "workers");
    }

    #[test]
    fn start_maps_to_redis_ids() {
        assert_eq!(StreamStart::New.as_id(), "$");
        assert_eq!(StreamStart::Beginning.as_id(), "0");
        assert_eq!(StreamStart::Id("5-0".into()).as_id(), "5-0");
    }

    #[test]
    fn reclaim_carries_min_idle() {
        let s = RedisStream::reclaim("orders", Duration::from_secs(30)).group("g");
        assert!(matches!(s.mode(), ReadMode::Reclaim { min_idle } if min_idle.as_secs() == 30));
    }
}
