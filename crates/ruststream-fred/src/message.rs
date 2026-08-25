//! Delivered-message wrapper that implements [`IncomingMessage`].

use std::fmt::{Debug, Formatter};
use std::time::Duration;

use bytes::Bytes;
use fred::clients::Pool;
use fred::interfaces::StreamsInterface;
use ruststream::runtime::RETRY_COUNT_HEADER;
use ruststream::{AckError, Headers, IncomingMessage, Partitioned, Positioned};

use crate::convert::fields_for_publish;
use crate::deadletter::{self, PoisonPolicy, REASON_DROPPED, REASON_MAX_DELIVERIES};
use crate::delay::{self, DelayConfig};
use crate::seek::{EntryId, RedisGroupPosition};

/// The well-known header key for per-message routing / partitioning.
///
/// Set this header on outgoing messages to control key-based fan-out when the runtime is
/// configured with `workers(N, by_key)`. The value is opaque bytes; the runtime hashes it to
/// assign a dispatch lane. Redis has no native partition concept on a single stream, so the key
/// travels as this header value and the sender is responsible for setting it.
pub const PARTITION_KEY_HEADER: &str = "redis-partition-key";

/// Everything a [`RedisMessage`] needs to settle itself against the stream it came from.
struct AckHandle {
    pool: Pool,
    key: String,
    group: String,
    id: String,
}

/// A Redis Streams delivery, read from a consumer group via `XREADGROUP` or `XAUTOCLAIM`.
///
/// Settlement follows the republish-retry model: `ack` is `XACK`; `nack(requeue = true)`
/// re-appends a copy of the entry to the same stream and then acks the original (at-least-once,
/// so a duplicate is possible if the process crashes between the two); `nack(requeue = false)`
/// acks the original to drop it.
pub struct RedisMessage {
    payload: Bytes,
    headers: Headers,
    ack: Option<AckHandle>,
    /// The parsed form of the entry id, kept beside the wire form so
    /// [`Positioned::position`] cannot fail. Both are derived from the same server-issued id.
    entry: EntryId,
    policy: PoisonPolicy,
    /// Set when the subscription opted into a durable ZSET delay queue; makes `nack_after` native.
    delay: Option<DelayConfig>,
}

impl Debug for RedisMessage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_struct("RedisMessage");
        s.field("payload_len", &self.payload.len());
        if let Some(ack) = &self.ack {
            s.field("key", &ack.key).field("id", &ack.id);
        }
        s.finish_non_exhaustive()
    }
}

impl RedisMessage {
    #[allow(
        clippy::too_many_arguments,
        reason = "internal constructor mirroring the descriptor"
    )]
    pub(crate) fn new(
        pool: Pool,
        key: String,
        group: String,
        id: String,
        entry: EntryId,
        payload: Bytes,
        headers: Headers,
        policy: PoisonPolicy,
        delay: Option<DelayConfig>,
    ) -> Self {
        Self {
            payload,
            headers,
            ack: Some(AckHandle {
                pool,
                key,
                group,
                id,
            }),
            entry,
            policy,
            delay,
        }
    }

    /// The stream entry ID (for example `1700000000000-0`) this message was read at.
    #[must_use]
    pub fn id(&self) -> Option<&str> {
        self.ack.as_ref().map(|a| a.id.as_str())
    }

    /// The parsed entry id this message was read at.
    #[must_use]
    pub const fn entry_id(&self) -> EntryId {
        self.entry
    }

    /// The consumer group this delivery was read through, or `None` once the message has settled.
    #[must_use]
    pub fn group(&self) -> Option<&str> {
        self.ack.as_ref().map(|a| a.group.as_str())
    }
}

impl Partitioned for RedisMessage {
    fn partition_key(&self) -> Option<&[u8]> {
        self.headers().get(PARTITION_KEY_HEADER)
    }
}

/// The position of a delivery is the group cursor that redelivers it.
///
/// The cursor is exclusive (a group resumes *after* the id it holds), so the pinned position is
/// the id immediately below this entry's - seeking to it delivers this message again, followed by
/// the entries after it. Repositioning is group-wide; see
/// [`RedisGroupSeeker`](crate::RedisGroupSeeker).
impl Positioned for RedisMessage {
    type Position = RedisGroupPosition;

    fn position(&self) -> RedisGroupPosition {
        RedisGroupPosition::after(self.entry.previous())
    }
}

impl IncomingMessage for RedisMessage {
    fn payload(&self) -> &[u8] {
        &self.payload
    }

    fn headers(&self) -> &Headers {
        &self.headers
    }

    // The runtime's keyed worker lanes read the key from here, not from `Partitioned`, so the
    // capability has to be wired through or `workers(n, by_key)` silently rotates every delivery.
    fn partition_key(&self) -> Option<&[u8]> {
        Partitioned::partition_key(self)
    }

    async fn ack(mut self) -> Result<(), AckError> {
        let handle = self.ack.take().expect("RedisMessage settled twice");
        xack(&handle).await
    }

    async fn nack(mut self, requeue: bool) -> Result<(), AckError> {
        let handle = self.ack.take().expect("RedisMessage settled twice");
        if requeue {
            if self.policy.is_active() {
                let next = next_retry_count(&self.headers);
                if self.policy.is_poison(next) {
                    // The framework retry-count reached the cap: dead-letter (or discard) instead
                    // of redelivering, then ack the original.
                    deadletter::settle_poison_stream(
                        &handle.pool,
                        &self.policy,
                        &self.payload,
                        &self.headers,
                        REASON_MAX_DELIVERIES,
                    )
                    .await
                    .map_err(broker_err)?;
                } else {
                    let mut headers = self.headers.clone();
                    headers.insert(RETRY_COUNT_HEADER, next.to_string());
                    republish(&handle, &self.payload, &headers).await?;
                }
            } else {
                // No poison policy: republish verbatim, the plain at-least-once retry.
                republish(&handle, &self.payload, &self.headers).await?;
            }
        } else if self.policy.is_active() {
            // Drop: dead-letter it (or discard when no dead-letter stream is set) before acking.
            deadletter::settle_poison_stream(
                &handle.pool,
                &self.policy,
                &self.payload,
                &self.headers,
                REASON_DROPPED,
            )
            .await
            .map_err(broker_err)?;
        }
        xack(&handle).await
    }

    /// Native delayed redelivery is available only when the subscription opted into a durable ZSET
    /// delay queue with [`RedisStream::delayed_retry`](crate::RedisStream::delayed_retry); otherwise
    /// the runtime applies its broker-agnostic deferred-republish fallback.
    fn supports_nack_after(&self) -> bool {
        self.delay.is_some()
    }

    /// Schedules the message for redelivery no sooner than `delay` from now via the configured ZSET
    /// delay queue (`ZADD` the delayed copy, then `XACK` the original), with the retry-count header
    /// incremented. The subscriber's sweeper re-`XADD`s it to the source stream once due.
    ///
    /// # Errors
    ///
    /// Returns [`AckError::Unsupported`] when the subscription did not opt into a delay queue, or
    /// [`AckError::Broker`] when the `ZADD` or `XACK` fails.
    async fn nack_after(mut self, delay: Duration) -> Result<(), AckError> {
        let handle = self.ack.take().expect("RedisMessage settled twice");
        let Some(cfg) = self.delay.as_ref() else {
            return Err(AckError::Unsupported);
        };
        // ZADD the delayed copy before XACK-ing the original, so a crash in between leaves a
        // duplicate (the scheduled copy plus the still-pending original) rather than a loss.
        delay::schedule(
            &handle.pool,
            cfg,
            &handle.id,
            &self.payload,
            &self.headers,
            delay,
        )
        .await?;
        xack(&handle).await
    }
}

/// The next framework retry-count value (the current header plus one, or one when absent).
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

/// Re-appends a copy of the message to the tail of its stream (the at-least-once retry). Runs before
/// the caller's `XACK` so a crash leaves a duplicate rather than a loss.
async fn republish(handle: &AckHandle, payload: &[u8], headers: &Headers) -> Result<(), AckError> {
    let fields = fields_for_publish(payload, headers);
    let _: String = handle
        .pool
        .xadd(handle.key.as_str(), false, None::<()>, "*", fields)
        .await
        .map_err(broker_err)?;
    Ok(())
}

async fn xack(handle: &AckHandle) -> Result<(), AckError> {
    let _: i64 = handle
        .pool
        .xack(
            handle.key.as_str(),
            handle.group.as_str(),
            handle.id.as_str(),
        )
        .await
        .map_err(broker_err)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::StreamContext;
    use fred::clients::Pool;
    use fred::types::config::Config;
    use ruststream::BuildContext;

    /// An unconnected pool (just client structs); `Pool::new` opens no sockets.
    fn offline_pool() -> Pool {
        Pool::new(Config::default(), None, None, None, 1).expect("offline pool")
    }

    fn delivery(id: &str) -> RedisMessage {
        RedisMessage::new(
            offline_pool(),
            "orders".to_owned(),
            "workers".to_owned(),
            id.to_owned(),
            id.parse().expect("valid entry id"),
            Bytes::from_static(b"{}"),
            Headers::new(),
            PoisonPolicy::default(),
            None,
        )
    }

    #[test]
    fn build_context_reads_entry_id_and_group() {
        let cx = StreamContext::build(&delivery("1700000000000-0"));
        assert_eq!(cx.entry_id(), Some("1700000000000-0"));
        assert_eq!(cx.consumer_group(), Some("workers"));
    }

    // The group cursor is exclusive, so the position that redelivers an entry sits one id below
    // it: pinning `<ms>-0` has to borrow from the millisecond half.
    #[test]
    fn position_pins_the_delivery_for_redelivery() {
        assert_eq!(
            delivery("1700000000000-4").position(),
            RedisGroupPosition::after(EntryId::new(1_700_000_000_000, 3))
        );
        assert_eq!(
            delivery("1700000000000-0").position(),
            RedisGroupPosition::after(EntryId::new(1_699_999_999_999, u64::MAX))
        );
    }
}
