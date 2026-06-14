//! Durable delayed retry backed by a Redis sorted-set (ZSET) delay queue.
//!
//! `retry_after(delay)` (a handler returning [`HandlerResult::retry_after`], or a delivery
//! `nack_after`-ed) asks the broker to redeliver a message no sooner than `delay` from now. Redis
//! Streams have no native per-message delay, so without this the runtime falls back to its
//! broker-agnostic deferred re-publish, which is at-most-once over the delay window (a process
//! crash before the timer fires loses the deferred copy).
//!
//! This module adds a crash-safe alternative, **opt-in and OFF by default**: when a subscription
//! names a ZSET key with [`RedisStream::delayed_retry`](crate::RedisStream::delayed_retry),
//! `nack_after` becomes native (`supports_nack_after` reports `true`). A delayed message is `ZADD`ed
//! to the named ZSET with score `fire_at = now + delay`, then the original is `XACK`ed; a sweeper
//! folded into the subscriber's read loop moves due entries (`score <= now`) back onto the source
//! stream with `XADD`. The entry lives in Redis across a crash, so redelivery survives a restart.
//!
//! Scores are wall-clock epoch milliseconds taken on the publishing process, so deployments should
//! keep clocks reasonably synced (NTP) - the same assumption any wall-clock delay queue makes.
//!
//! [`HandlerResult::retry_after`]: ruststream::runtime::HandlerResult::retry_after

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use fred::clients::Pool;
use fred::interfaces::{KeysInterface, SortedSetsInterface, StreamsInterface};
use ruststream::runtime::RETRY_COUNT_HEADER;
use ruststream::{AckError, Headers};

use crate::convert::fields_for_publish;
use crate::envelope::{frame, unframe};
use crate::error::RedisError;

/// How many due entries one sweep pass claims and re-publishes before yielding back to the read
/// loop. Bounds the work a single fetch does so a large backlog cannot stall fresh delivery.
const SWEEP_BATCH: i64 = 128;

/// How a subscription should handle `retry_after` / `nack_after` delays.
///
/// Passed to [`RedisStream::delayed_retry`](crate::RedisStream::delayed_retry). There is no default
/// that enables it: a delay queue costs extra Redis keys, memory, and the polling sweeper, so a user
/// opts in and names the ZSET key explicitly (the key has no sane default).
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use ruststream_fred::{DelayedRetry, RedisStream};
///
/// let sub = RedisStream::new("orders").group("workers").delayed_retry(
///     DelayedRetry::DurableZset { key: "orders.delayed".to_owned(), ttl: None },
/// );
/// # let _ = sub;
/// ```
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum DelayedRetry {
    /// Schedule delayed redeliveries in the named ZSET.
    ///
    /// `key` is the ZSET delay-queue key (required, no default). `ttl`, when set, `PEXPIRE`s the
    /// key on every write so an abandoned queue cleans itself up; it **must exceed the longest
    /// scheduled `retry_after` delay**, or pending entries are dropped before they fire.
    DurableZset {
        /// The ZSET delay-queue key.
        key: String,
        /// Optional auto-cleanup TTL on the ZSET key. Must exceed the longest scheduled delay.
        ttl: Option<Duration>,
    },
}

/// The resolved delay-queue settings a subscriber and its messages carry. Cheap to clone.
#[derive(Debug, Clone)]
pub(crate) struct DelayConfig {
    zset_key: String,
    ttl: Option<Duration>,
}

impl DelayConfig {
    pub(crate) fn from_retry(retry: &DelayedRetry) -> Self {
        match retry {
            DelayedRetry::DurableZset { key, ttl } => Self {
                zset_key: key.clone(),
                ttl: *ttl,
            },
        }
    }
}

/// Current wall-clock time as epoch milliseconds (the ZSET score space).
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

fn delay_millis(delay: Duration) -> u64 {
    u64::try_from(delay.as_millis()).unwrap_or(u64::MAX)
}

/// Epoch-millisecond timestamps stay well under 2^53, so representing one as the f64 score a ZSET
/// uses is lossless in practice.
#[allow(
    clippy::cast_precision_loss,
    reason = "epoch-ms < 2^53 is exact in f64"
)]
fn as_score(ms: u64) -> f64 {
    ms as f64
}

/// Positive millisecond count for `PEXPIRE`, clamped up to 1 (a `PEXPIRE 0` deletes the key).
fn ttl_millis(ttl: Duration) -> i64 {
    i64::try_from(ttl.as_millis()).unwrap_or(i64::MAX).max(1)
}

/// Packs an entry for a ZSET member: a length-prefixed delivery id (a uniqueness salt, so two
/// byte-identical payloads do not collide into one member) followed by the lossless header/payload
/// frame. The id is not reused on redelivery; the sweep re-`XADD`s under a fresh id.
fn encode_member(id: &str, payload: &[u8], headers: &Headers) -> Vec<u8> {
    let body = frame(None, payload, headers);
    let id = id.as_bytes();
    let id_len = u32::try_from(id.len()).unwrap_or(u32::MAX);
    let mut buf = Vec::with_capacity(4 + id.len() + body.len());
    buf.extend_from_slice(&id_len.to_be_bytes());
    buf.extend_from_slice(id);
    buf.extend_from_slice(&body);
    buf
}

/// Reverses [`encode_member`], dropping the id salt and returning the payload and headers.
fn decode_member(member: &[u8]) -> Option<(Bytes, Headers)> {
    let id_len = u32::from_be_bytes(member.get(0..4)?.try_into().ok()?) as usize;
    let body = member.get(4usize.checked_add(id_len)?..)?;
    Some(unframe(None, body))
}

fn next_retry_count(headers: &Headers) -> u64 {
    headers
        .get_str(RETRY_COUNT_HEADER)
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0)
        + 1
}

fn broker_err(err: fred::error::Error) -> AckError {
    AckError::Broker(Box::new(err))
}

/// `ZADD`s a delayed copy of the message (retry count incremented) at `now + delay`, refreshing the
/// optional key TTL. The caller `XACK`s the original afterwards, so a crash in between leaves the
/// scheduled copy (a duplicate) rather than losing the message.
pub(crate) async fn schedule(
    pool: &Pool,
    cfg: &DelayConfig,
    id: &str,
    payload: &[u8],
    headers: &Headers,
    delay: Duration,
) -> Result<(), AckError> {
    let fire_at = now_ms().saturating_add(delay_millis(delay));

    let mut next = headers.clone();
    next.insert(RETRY_COUNT_HEADER, next_retry_count(headers).to_string());
    let member = encode_member(id, payload, &next);

    let _: i64 = pool
        .zadd(
            cfg.zset_key.as_str(),
            None,
            None,
            false,
            false,
            (as_score(fire_at), member),
        )
        .await
        .map_err(broker_err)?;
    if let Some(ttl) = cfg.ttl {
        let _: i64 = pool
            .pexpire(cfg.zset_key.as_str(), ttl_millis(ttl), None)
            .await
            .map_err(broker_err)?;
    }
    Ok(())
}

/// Moves entries whose `fire_at` has passed from the delay ZSET back onto `stream_key`.
///
/// Each due member is claimed with `ZREM`: only the consumer whose `ZREM` removes it (returns 1)
/// re-`XADD`s it, so concurrent sweepers never double-publish. Bounded to [`SWEEP_BATCH`] entries
/// per pass.
pub(crate) async fn sweep_due(
    pool: &Pool,
    cfg: &DelayConfig,
    stream_key: &str,
) -> Result<(), RedisError> {
    let now = as_score(now_ms());
    let due: Vec<Bytes> = pool
        .zrangebyscore(
            cfg.zset_key.as_str(),
            0.0,
            now,
            false,
            Some((0, SWEEP_BATCH)),
        )
        .await
        .map_err(RedisError::stream)?;

    for member in due {
        let removed: i64 = pool
            .zrem(cfg.zset_key.as_str(), member.clone())
            .await
            .map_err(RedisError::stream)?;
        // Another sweeper already claimed and re-published this entry.
        if removed != 1 {
            continue;
        }
        let Some((payload, headers)) = decode_member(&member) else {
            continue;
        };
        let fields = fields_for_publish(&payload, &headers);
        let _: String = pool
            .xadd(stream_key, false, None::<()>, "*", fields)
            .await
            .map_err(RedisError::stream)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn member_round_trips_payload_and_headers() {
        let mut headers = Headers::new();
        headers.insert("content-type", "application/json");
        headers.insert(RETRY_COUNT_HEADER, "2");

        let member = encode_member("1700000000000-0", b"{}", &headers);
        let (payload, decoded) = decode_member(&member).expect("decodes");
        assert_eq!(payload.as_ref(), b"{}");
        assert_eq!(decoded.content_type(), Some("application/json"));
        assert_eq!(decoded.get_str(RETRY_COUNT_HEADER), Some("2"));
    }

    #[test]
    fn distinct_ids_yield_distinct_members_for_equal_payloads() {
        let headers = Headers::new();
        let a = encode_member("1-0", b"dup", &headers);
        let b = encode_member("2-0", b"dup", &headers);
        assert_ne!(
            a, b,
            "the id salt must keep equal payloads from colliding in the ZSET"
        );
    }

    #[test]
    fn next_retry_count_starts_at_one_and_increments() {
        let mut headers = Headers::new();
        assert_eq!(next_retry_count(&headers), 1);
        headers.insert(RETRY_COUNT_HEADER, "4");
        assert_eq!(next_retry_count(&headers), 5);
        // A malformed counter restarts from zero rather than panicking.
        headers.insert(RETRY_COUNT_HEADER, "not-a-number");
        assert_eq!(next_retry_count(&headers), 1);
    }

    #[test]
    fn ttl_millis_clamps_sub_millisecond_to_one() {
        assert_eq!(ttl_millis(Duration::from_secs(30)), 30_000);
        assert_eq!(ttl_millis(Duration::ZERO), 1);
    }
}
