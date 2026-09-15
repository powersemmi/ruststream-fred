//! Delivered-message wrapper that implements [`IncomingMessage`].

use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use fred::clients::Pool;
use fred::interfaces::StreamsInterface;
use ruststream::{AckError, HeaderMap, IncomingMessage, Partitioned, Positioned};

use crate::convert::fields_for_publish;
use crate::delay::{self, DelayConfig};
use crate::seek::{EntryId, RedisGroupPosition, RedisGroupSeeker};
use crate::stream::RequeueMode;

/// The well-known header key for per-message routing / partitioning.
///
/// The partition key controls key-based fan-out when the runtime is configured with
/// `workers(N, by_key)`. The value is opaque bytes; the runtime hashes it to assign a dispatch
/// lane. Redis has no native partition concept, so the key travels as this header value on all
/// three transports.
///
/// A publish sets it with the [`partition_key`](crate::RedisPublishSteps::partition_key) step,
/// which resolves into this header. Writing the header at the call site still works, and is the
/// spelling that carries across brokers.
pub const PARTITION_KEY_HEADER: &str = "redis-partition-key";

/// Header carrying the native Redis Streams delivery count, so a handler can branch on how many
/// attempts a message has already had.
///
/// Written by the two read modes that claim: a [`reclaim`](crate::RedisStream::reclaim) delivery
/// reports the count including itself, a [`claiming`](crate::RedisStream::claiming) delivery the
/// attempts made before it (zero on a fresh entry). The
/// [`redelivery_count`](IncomingMessage::redelivery_count) the runtime reads normalises the two,
/// counting this delivery on both.
pub const DELIVERY_COUNT_HEADER: &str = "redis-delivery-count";

/// Header carrying how long (milliseconds) the delivery had been pending before this read. Zero on
/// an entry a [`claiming`](crate::RedisStream::claiming) subscription read off the tail.
pub const IDLE_MS_HEADER: &str = "redis-idle-ms";

/// Everything a [`RedisMessage`] needs to settle itself against the stream it came from.
struct AckHandle {
    pool: Pool,
    key: String,
    group: String,
    id: String,
}

/// A Redis Streams delivery, read from a consumer group via `XREADGROUP` or `XAUTOCLAIM`.
///
/// Settlement follows the republish-retry model: `ack` is `XACK`; a retry re-appends a copy of the
/// entry to the same stream and then acks the original (at-least-once, so a duplicate is possible
/// if the process crashes between the two); a drop acks the original.
///
/// On a [`claiming`](crate::RedisStream::claiming) subscription the retry is the read mode's own:
/// the entry stays in the pending entries list, and the subscription's next read claims it back
/// with the server's delivery count one higher, which is the count this delivery reports.
pub struct RedisMessage {
    payload: Bytes,
    headers: HeaderMap,
    ack: Option<AckHandle>,
    /// The parsed form of the entry id, kept beside the wire form so
    /// [`Positioned::position`] cannot fail. Both are derived from the same server-issued id.
    entry: EntryId,
    /// Set when the subscription opted into a durable ZSET delay queue; makes `nack_after` native.
    delay: Option<DelayConfig>,
    /// The server's own delivery count, counting this delivery, on the read modes that report one.
    delivered: Option<u64>,
    /// The subscription's reposition handle, minted once when it opened. Shared rather than
    /// rebuilt, so carrying it costs one reference-count bump per delivery.
    seeker: Arc<RedisGroupSeeker>,
    /// What `nack(requeue = true)` does here, which the subscription's read mode decides.
    requeue: RequeueMode,
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
        headers: HeaderMap,
        delay: Option<DelayConfig>,
        delivered: Option<u64>,
        seeker: Arc<RedisGroupSeeker>,
        requeue: RequeueMode,
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
            delay,
            delivered,
            seeker,
            requeue,
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

    /// The subscription's reposition handle, for the per-delivery context to carry.
    pub(crate) fn seeker(&self) -> &RedisGroupSeeker {
        &self.seeker
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

    fn headers(&self) -> &HeaderMap {
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

    /// How many times the server has delivered this entry, counting this delivery.
    ///
    /// Answered on the two read modes that claim, from the count Redis keeps in the pending
    /// entries list: [`claiming`](crate::RedisStream::claiming) is told the count as of the
    /// previous delivery, so this one is added to it, and [`reclaim`](crate::RedisStream::reclaim)
    /// reads `XPENDING` after the claim, where this delivery is already counted. The fresh tail
    /// claims nothing and has no count of its own, so a cap on it is the runtime's header instead.
    fn redelivery_count(&self) -> Option<u64> {
        self.delivered
    }

    async fn nack(mut self, requeue: bool) -> Result<(), AckError> {
        let handle = self.ack.take().expect("RedisMessage settled twice");
        if requeue && let RequeueMode::LeavePending { .. } = self.requeue {
            // The claiming mode retries through the pending entries list: no copy is appended and
            // the original is not acked, so the subscription's next read claims it back once it
            // has been idle `min_idle`, with the server's delivery count one higher.
            drop(handle);
            return Ok(());
        }
        if requeue {
            republish(&handle, &self.payload, &self.headers).await?;
        }
        xack(&handle).await
    }

    /// Whether a delay reaches Redis rather than the runtime's deferred copy.
    ///
    /// True on a subscription that named a ZSET delay queue with
    /// [`RedisStream::delayed_retry`](crate::RedisStream::delayed_retry), and on a
    /// [`claiming`](crate::RedisStream::claiming) subscription, whose pending entries list is a
    /// delayed redelivery of Redis's own. Elsewhere the runtime publishes the copy, which the
    /// mount site can customise with `out_retry`.
    fn supports_nack_after(&self) -> bool {
        self.delay.is_some() || matches!(self.requeue, RequeueMode::LeavePending { .. })
    }

    /// Holds the message back for `delay` and redelivers it.
    ///
    /// A ZSET delay queue serves the delay exactly: the delayed copy is `ZADD`ed and the original
    /// `XACK`ed, and the subscriber's sweeper re-`XADD`s it once due, so the retry survives a
    /// crash. Without one, a claiming subscription leaves the entry pending and claims it back on
    /// its own `min_idle`, which is the granularity Redis offers there: a shorter delay waits
    /// `min_idle`, a longer one is honoured by the read that finds the entry still idle.
    ///
    /// # Errors
    ///
    /// Returns [`AckError::Unsupported`] when the subscription has neither, or
    /// [`AckError::Broker`] when the `ZADD` or `XACK` fails.
    async fn nack_after(mut self, delay: Duration) -> Result<(), AckError> {
        let handle = self.ack.take().expect("RedisMessage settled twice");
        let Some(cfg) = self.delay.as_ref() else {
            if let RequeueMode::LeavePending { .. } = self.requeue {
                drop(handle);
                return Ok(());
            }
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

fn broker_err(err: fred::error::Error) -> AckError {
    AckError::Broker(Box::new(err))
}

/// Re-appends a copy of the message to the tail of its stream (the at-least-once retry). Runs before
/// the caller's `XACK` so a crash leaves a duplicate rather than a loss.
async fn republish(
    handle: &AckHandle,
    payload: &[u8],
    headers: &HeaderMap,
) -> Result<(), AckError> {
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
    use std::sync::atomic::AtomicU64;

    use super::*;
    use crate::context::keys::{ConsumerGroup, EntryId as EntryIdKey, Position, SeekHandle};
    use crate::context::{StreamBatchContext, StreamContext};
    use fred::clients::Pool;
    use fred::types::config::Config;
    use ruststream::{BuildBatchContext, BuildContext, Field};

    /// An unconnected pool (just client structs); `Pool::new` opens no sockets.
    fn offline_pool() -> Pool {
        Pool::new(Config::default(), None, None, None, 1).expect("offline pool")
    }

    fn delivery(id: &str) -> RedisMessage {
        let pool = offline_pool();
        let seeker = Arc::new(RedisGroupSeeker::new(
            pool.clone(),
            "orders",
            "workers",
            Arc::new(AtomicU64::new(0)),
        ));
        RedisMessage::new(
            pool,
            "orders".to_owned(),
            "workers".to_owned(),
            id.to_owned(),
            id.parse().expect("valid entry id"),
            Bytes::from_static(b"{}"),
            HeaderMap::new(),
            None,
            None,
            seeker,
            RequeueMode::Republish,
        )
    }

    #[test]
    fn build_context_reads_the_native_fields() {
        let cx = StreamContext::build(&delivery("1700000000000-0"));
        assert_eq!(EntryIdKey.get(&cx), EntryId::new(1_700_000_000_000, 0));
        assert_eq!(ConsumerGroup.get(&cx), "workers");
        assert_eq!(SeekHandle.get(&cx).key(), "orders");
    }

    // The seek key is what a handler repositions through, so the context has to hand back the
    // subscription's own handle rather than one rebuilt off the delivery.
    #[test]
    fn build_context_carries_the_position_that_redelivers() {
        let cx = StreamContext::build(&delivery("1700000000000-4"));
        assert_eq!(
            Position.get(&cx),
            RedisGroupPosition::after(EntryId::new(1_700_000_000_000, 3))
        );
    }

    // A batch spans many deliveries, so its context carries only what the subscription shares: the
    // group and its cursor handle, never one delivery's entry id or position.
    #[test]
    fn build_batch_context_carries_only_subscription_scoped_fields() {
        let cx = StreamBatchContext::build(&delivery("1700000000000-0"));
        assert_eq!(ConsumerGroup.get(&cx), "workers");
        assert_eq!(SeekHandle.get(&cx).group(), "workers");
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
