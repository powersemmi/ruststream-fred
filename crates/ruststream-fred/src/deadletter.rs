//! Dead-letter routing and a delivery-count poison cap for the transports that can acknowledge
//! (Redis Streams and the reliable List).
//!
//! A message can fail to process indefinitely: a handler keeps `nack`-ing it (the framework
//! retry-count loop) or it keeps being fetched but never acked across crashes (the native stream
//! delivery-count loop). Without a cap it is redelivered forever, and a plain
//! `nack(requeue = false)` silently discards it.
//!
//! Two opt-in, off-by-default settings address this on a [`RedisStream`](crate::RedisStream) or a
//! reliable [`RedisList`](crate::RedisList):
//!
//! * `dead_letter(key)` - on drop / poison, copy the message to the named key (same transport
//!   family: stream to stream, list to list) instead of discarding it, tagged with a
//!   [`DEAD_LETTER_REASON_HEADER`].
//! * `max_deliveries(n)` - cap the delivery count; exceeding it dead-letters (or discards) the
//!   message instead of redelivering.
//!
//! The copy is written before the original is acked (`XADD`/`LPUSH` before `XACK`/`LREM`), so a
//! crash in between leaves a duplicate in the dead-letter store rather than losing the message.
//! Simple List and Pub/Sub cannot ack, so they have no dead-letter path.

use fred::clients::Pool;
use fred::interfaces::StreamsInterface;
use ruststream::Headers;

use crate::convert::fields_for_publish;

/// Header naming why a message was dead-lettered: [`REASON_DROPPED`] or [`REASON_MAX_DELIVERIES`].
pub const DEAD_LETTER_REASON_HEADER: &str = "x-dead-letter-reason";
/// Header exposing the native Redis Streams delivery count on a reclaimed delivery, so a handler can
/// branch or dead-letter manually.
pub const DELIVERY_COUNT_HEADER: &str = "redis-delivery-count";
/// Header exposing how long (milliseconds) a reclaimed delivery had been pending.
pub const IDLE_MS_HEADER: &str = "redis-idle-ms";

/// [`DEAD_LETTER_REASON_HEADER`] value for a `nack(requeue = false)` / drop.
pub(crate) const REASON_DROPPED: &str = "dropped";
/// [`DEAD_LETTER_REASON_HEADER`] value for exceeding `max_deliveries`.
pub(crate) const REASON_MAX_DELIVERIES: &str = "max-deliveries";

/// Resolved dead-letter / poison-cap settings a subscriber and its deliveries carry. Cheap to clone.
#[derive(Debug, Clone, Default)]
pub(crate) struct PoisonPolicy {
    pub(crate) dead_letter: Option<String>,
    pub(crate) max_deliveries: Option<u64>,
}

impl PoisonPolicy {
    /// Whether either setting is configured (otherwise settlement keeps its plain behaviour).
    pub(crate) const fn is_active(&self) -> bool {
        self.dead_letter.is_some() || self.max_deliveries.is_some()
    }

    /// Whether a delivery count has reached the cap (so the message is poison).
    pub(crate) fn is_poison(&self, count: u64) -> bool {
        self.max_deliveries.is_some_and(|max| count >= max)
    }

    pub(crate) fn dead_letter_key(&self) -> Option<&str> {
        self.dead_letter.as_deref()
    }
}

/// Returns the headers for a dead-lettered copy: the originals plus the reason tag.
pub(crate) fn with_reason(headers: &Headers, reason: &'static str) -> Headers {
    let mut tagged = headers.clone();
    tagged.insert(DEAD_LETTER_REASON_HEADER, reason);
    tagged
}

/// Routes a message to its dead-letter stream when one is configured, else does nothing (the caller
/// then `XACK`s, discarding it). `XADD` runs before the caller's `XACK`, so a crash leaves a
/// duplicate rather than a loss.
///
/// # Errors
///
/// Returns the underlying `fred` error when the `XADD` fails.
pub(crate) async fn settle_poison_stream(
    pool: &Pool,
    policy: &PoisonPolicy,
    payload: &[u8],
    headers: &Headers,
    reason: &'static str,
) -> Result<(), fred::error::Error> {
    if let Some(dlq) = policy.dead_letter_key() {
        let fields = fields_for_publish(payload, &with_reason(headers, reason));
        let _: String = pool.xadd(dlq, false, None::<()>, "*", fields).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_poison_only_when_count_reaches_the_cap() {
        let policy = PoisonPolicy {
            dead_letter: None,
            max_deliveries: Some(3),
        };
        assert!(!policy.is_poison(2));
        assert!(policy.is_poison(3));
        assert!(policy.is_poison(4));
        assert!(policy.is_active());
    }

    #[test]
    fn no_cap_is_never_poison() {
        let policy = PoisonPolicy::default();
        assert!(!policy.is_poison(u64::MAX));
        assert!(!policy.is_active());
    }

    #[test]
    fn with_reason_tags_without_dropping_originals() {
        let mut headers = Headers::new();
        headers.insert("content-type", "application/json");
        let tagged = with_reason(&headers, REASON_DROPPED);
        assert_eq!(tagged.get_str(DEAD_LETTER_REASON_HEADER), Some("dropped"));
        assert_eq!(tagged.content_type(), Some("application/json"));
    }
}
