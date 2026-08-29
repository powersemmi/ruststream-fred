//! The per-message partition key, carried by a publisher adapter.
//!
//! Redis has no native partition concept, so the key travels as the well-known
//! [`PARTITION_KEY_HEADER`] header. [`RedisPublishExt::partition_key`] wraps a publisher in an
//! adapter that offers the key as its [base headers](ruststream::Publisher::base_headers), so it
//! sits underneath whatever the publish itself names.

use ruststream::{HeaderMap, OutgoingMessage, Publisher};

use crate::list::RedisListPublisher;
use crate::message::PARTITION_KEY_HEADER;
use crate::publisher::RedisPublisher;
use crate::pubsub::RedisPubSubPublisher;

/// A publisher adapter that carries a partition key under every message sent through it.
///
/// Produced by [`RedisPublishExt::partition_key`]. It borrows the publisher it wraps and is a
/// [`Publisher`] itself, so the whole core publish builder is available on it.
///
/// The key rides underneath the publish's own headers: a call naming [`PARTITION_KEY_HEADER`]
/// overrides it, and it survives a call that names any other header.
///
/// # Examples
///
/// ```no_run
/// use ruststream::runtime::PublishExt;
/// use ruststream::{Broker, Publisher};
/// use ruststream_fred::{RedisBroker, RedisPublishExt};
///
/// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
/// let connected = RedisBroker::standalone("redis://localhost:6379").connect().await?;
/// let keyed = connected.publisher();
/// let keyed = keyed.partition_key("tenant-a");
/// keyed.raw(b"{}").to("orders").publish().await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
#[must_use = "a keyed publisher does nothing until something is published through it"]
pub struct PartitionKeyed<'a, P: ?Sized> {
    inner: &'a P,
    // Built once at construction; the builder borrows it per publish. Do not move this back into
    // `publish`, which would clone a header map on the path every message takes.
    base: HeaderMap,
}

impl<P: ?Sized> PartitionKeyed<'_, P> {
    /// The partition key carried under every message published through this handle.
    #[must_use]
    pub fn key(&self) -> &[u8] {
        // The constructor always writes this entry, so the fallback is unreachable.
        self.base.get(PARTITION_KEY_HEADER).unwrap_or(&[])
    }
}

impl<P: Publisher + ?Sized> Publisher for PartitionKeyed<'_, P> {
    type Error = P::Error;

    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        self.inner.publish(msg).await
    }

    fn base_headers(&self) -> Option<&HeaderMap> {
        Some(&self.base)
    }
}

/// The Redis-specific publisher adapters, applied ahead of the publish builder.
///
/// Import it to reach [`partition_key`](Self::partition_key) on any of this crate's publishers.
/// The trait is bound to those types, so the adapter does not appear on another broker's
/// publisher.
///
/// # Examples
///
/// ```no_run
/// use ruststream::runtime::PublishExt;
/// use ruststream::{Broker, Outgoing, Publisher};
/// use ruststream_fred::{RedisBroker, RedisPublishExt};
/// use serde::Serialize;
///
/// #[derive(Outgoing, Serialize)]
/// #[outgoing(name = "orders")]
/// struct Order {
///     id: u64,
/// }
///
/// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
/// let connected = RedisBroker::standalone("redis://localhost:6379").connect().await?;
/// let publisher = connected.publisher();
///
/// // Every order of one tenant lands in the same `workers(n, by_key)` lane, so their
/// // relative order is preserved while other tenants run in parallel.
/// let tenant = publisher.partition_key("tenant-a");
/// tenant.message(&Order { id: 7 }).publish().await?;
/// tenant.message(&Order { id: 8 }).publish().await?;
/// # Ok(())
/// # }
/// ```
pub trait RedisPublishExt: Publisher {
    /// Returns an adapter carrying `key` as the partition key of everything sent through it.
    ///
    /// The key feeds the runtime's keyed worker lanes (`workers(n, by_key)`): deliveries sharing a
    /// key are dispatched to the same lane, so their relative order survives concurrency. It
    /// travels as [`PARTITION_KEY_HEADER`], underneath the publish's own headers, so a call naming
    /// that header itself overrides the handle's key.
    ///
    /// `key` is copied into the adapter, so it need not outlive the publisher.
    fn partition_key<K>(&self, key: &K) -> PartitionKeyed<'_, Self>
    where
        K: AsRef<[u8]> + ?Sized,
    {
        let mut base = HeaderMap::new();
        base.insert(PARTITION_KEY_HEADER, key.as_ref().to_vec());
        PartitionKeyed { inner: self, base }
    }
}

// Listed per type rather than blanket over `Publisher`, which would grow the adapter on every
// other broker's publisher. Not implemented for `PartitionKeyed`, so re-keying does not compile.
impl RedisPublishExt for RedisPublisher {}
impl RedisPublishExt for RedisListPublisher {}
impl RedisPublishExt for RedisPubSubPublisher {}

#[cfg(feature = "testing")]
impl RedisPublishExt for crate::testing::RedisTestPublisher {}
