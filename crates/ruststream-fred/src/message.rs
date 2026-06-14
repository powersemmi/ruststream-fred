//! Delivered-message wrapper that implements [`IncomingMessage`].

use std::fmt::{Debug, Formatter};

use bytes::Bytes;
use fred::clients::Pool;
use fred::interfaces::StreamsInterface;
use ruststream::{AckError, Headers, IncomingMessage, Partitioned};

use crate::convert::fields_for_publish;

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
    pub(crate) fn new(
        pool: Pool,
        key: String,
        group: String,
        id: String,
        payload: Bytes,
        headers: Headers,
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
        }
    }

    /// The stream entry ID (for example `1700000000000-0`) this message was read at.
    #[must_use]
    pub fn id(&self) -> Option<&str> {
        self.ack.as_ref().map(|a| a.id.as_str())
    }
}

impl Partitioned for RedisMessage {
    fn partition_key(&self) -> Option<&[u8]> {
        self.headers().get(PARTITION_KEY_HEADER)
    }
}

impl IncomingMessage for RedisMessage {
    fn payload(&self) -> &[u8] {
        &self.payload
    }

    fn headers(&self) -> &Headers {
        &self.headers
    }

    async fn ack(mut self) -> Result<(), AckError> {
        let handle = self.ack.take().expect("RedisMessage settled twice");
        xack(&handle).await
    }

    async fn nack(mut self, requeue: bool) -> Result<(), AckError> {
        let handle = self.ack.take().expect("RedisMessage settled twice");
        if requeue {
            // Republish a copy to the tail before acking the original, so a crash in between
            // leaves a duplicate rather than losing the message (at-least-once).
            let fields = fields_for_publish(&self.payload, &self.headers);
            let _: String = handle
                .pool
                .xadd(handle.key.as_str(), false, None::<()>, "*", fields)
                .await
                .map_err(|err| AckError::Broker(Box::new(err)))?;
        }
        xack(&handle).await
    }
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
        .map_err(|err| AckError::Broker(Box::new(err)))?;
    Ok(())
}
