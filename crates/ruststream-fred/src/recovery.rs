//! Orphan recovery for reliable lists, backed by an opt-in ZSET watchdog.
//!
//! A reliable list ([`RedisList::reliable`](crate::RedisList::reliable)) `LMOVE`s each entry to a
//! processing list and `LREM`s it on settle. Redis lists have no native idle/pending tracking, so a
//! consumer that dies after the `LMOVE` but before settling strands its entry on the processing list
//! forever.
//!
//! This module adds a watchdog, **opt-in and OFF by default**: when a subscription names a recovery
//! ZSET key with [`RedisList::recovery_zset`](crate::RedisList::recovery_zset) (and a
//! [`min_idle`](crate::RedisList::min_idle)), each claimed entry is recorded in the ZSET keyed by a
//! per-claim member (score = claim time). A sweeper folded into the subscriber's read loop moves
//! entries idle longer than `min_idle` back onto the main list with `LPUSH`, where a live consumer
//! re-claims them.
//!
//! Each claim is a distinct ZSET member (a process id plus a per-process counter prefix the raw
//! value), so two in-flight entries with byte-identical values are tracked and recovered separately
//! rather than collapsing into one. Recovery is at-least-once: if `min_idle` is shorter than a
//! legitimate handler runtime, a still-running entry can be recovered and reprocessed, so set it
//! above the longest handler runtime, exactly as for the Streams reclaim path.
//!
//! Streams remain the recommended durable path; this is the lightweight upgrade for reliable lists.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use fred::clients::Pool;
use fred::interfaces::{KeysInterface, ListInterface, SortedSetsInterface};
use ruststream::AckError;

use crate::error::RedisError;

/// How many orphaned entries one sweep pass recovers before yielding back to the read loop.
const SWEEP_BATCH: i64 = 128;
/// Length of the per-claim uniqueness salt: a 4-byte process id plus an 8-byte counter.
const SALT_LEN: usize = 12;

/// Resolved recovery settings a reliable-list subscriber and its ack handles carry. Cheap to clone.
#[derive(Debug, Clone)]
pub(crate) struct RecoveryConfig {
    pub(crate) zset_key: String,
    pub(crate) min_idle: Duration,
    pub(crate) ttl: Option<Duration>,
}

/// Current wall-clock time as epoch milliseconds (the ZSET score space).
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
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

fn millis(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

/// Positive millisecond count for `PEXPIRE`, clamped up to 1 (a `PEXPIRE 0` deletes the key).
fn ttl_millis(ttl: Duration) -> i64 {
    i64::try_from(ttl.as_millis()).unwrap_or(i64::MAX).max(1)
}

/// Builds a unique ZSET member for a claim: a process id plus a per-process counter, then the raw
/// list value. The salt keeps two byte-identical values from collapsing into a single member.
fn claim_member(value: &[u8]) -> Vec<u8> {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut buf = Vec::with_capacity(SALT_LEN + value.len());
    buf.extend_from_slice(&std::process::id().to_be_bytes());
    buf.extend_from_slice(&n.to_be_bytes());
    buf.extend_from_slice(value);
    buf
}

/// Recovers the raw list value from a claim member (drops the [`SALT_LEN`]-byte salt).
fn value_from_member(member: &[u8]) -> Option<&[u8]> {
    member.get(SALT_LEN..)
}

fn broker_err(err: fred::error::Error) -> AckError {
    AckError::Broker(Box::new(err))
}

/// Records a freshly claimed entry in the recovery ZSET (score = now), refreshing the optional TTL.
///
/// Returns the ZSET member to stash on the ack handle, so settlement removes exactly this claim.
pub(crate) async fn record_claim(
    pool: &Pool,
    cfg: &RecoveryConfig,
    value: &[u8],
) -> Result<Vec<u8>, RedisError> {
    let member = claim_member(value);
    let _: i64 = pool
        .zadd(
            cfg.zset_key.as_str(),
            None,
            None,
            false,
            false,
            (as_score(now_ms()), member.clone()),
        )
        .await
        .map_err(RedisError::stream)?;
    if let Some(ttl) = cfg.ttl {
        let _: i64 = pool
            .pexpire(cfg.zset_key.as_str(), ttl_millis(ttl), None)
            .await
            .map_err(RedisError::stream)?;
    }
    Ok(member)
}

/// Drops a settled claim's tracking from the recovery ZSET. Best-effort: a claim already swept by
/// the watchdog is simply absent.
pub(crate) async fn forget(pool: &Pool, zset_key: &str, member: &[u8]) -> Result<(), AckError> {
    let _: i64 = pool
        .zrem(zset_key, member.to_vec())
        .await
        .map_err(broker_err)?;
    Ok(())
}

/// Moves entries idle longer than `min_idle` from the processing list back to the main list.
///
/// For each due member: `LREM` the value from the processing list; the consumer whose `LREM`
/// removes it (returns 1) owns the recovery and `LPUSH`es it back to main. The member is `ZREM`'d
/// either way, so stale tracking does not pile up.
pub(crate) async fn sweep_orphans(
    pool: &Pool,
    cfg: &RecoveryConfig,
    main_key: &str,
    processing_key: &str,
) -> Result<(), RedisError> {
    let cutoff = as_score(now_ms().saturating_sub(millis(cfg.min_idle)));
    let due: Vec<Bytes> = pool
        .zrangebyscore(
            cfg.zset_key.as_str(),
            0.0,
            cutoff,
            false,
            Some((0, SWEEP_BATCH)),
        )
        .await
        .map_err(RedisError::stream)?;

    for member in due {
        if let Some(value) = value_from_member(&member) {
            let value = value.to_vec();
            let removed: i64 = pool
                .lrem(processing_key, 1, value.clone())
                .await
                .map_err(RedisError::stream)?;
            if removed == 1 {
                let _: i64 = pool
                    .lpush(main_key, value)
                    .await
                    .map_err(RedisError::stream)?;
            }
        }
        let _: i64 = pool
            .zrem(cfg.zset_key.as_str(), member.to_vec())
            .await
            .map_err(RedisError::stream)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn member_carries_value_after_the_salt() {
        let member = claim_member(b"job-payload");
        assert_eq!(member.len(), SALT_LEN + b"job-payload".len());
        assert_eq!(value_from_member(&member), Some(b"job-payload".as_slice()));
    }

    #[test]
    fn equal_values_get_distinct_members() {
        let a = claim_member(b"dup");
        let b = claim_member(b"dup");
        assert_ne!(
            a, b,
            "the per-claim salt must keep equal values from colliding"
        );
        assert_eq!(value_from_member(&a), value_from_member(&b));
    }

    #[test]
    fn short_member_has_no_value() {
        assert_eq!(value_from_member(&[0u8; SALT_LEN - 1]), None);
    }

    #[test]
    fn ttl_millis_clamps_sub_millisecond_to_one() {
        assert_eq!(ttl_millis(Duration::from_secs(30)), 30_000);
        assert_eq!(ttl_millis(Duration::ZERO), 1);
    }
}
