//! The per-message partition key, carried by a publisher adapter.
//!
//! Redis has no native partition concept, so the key travels as the well-known
//! [`PARTITION_KEY_HEADER`] header and the sender sets it. Writing that header by hand is the one
//! outgoing knob that cannot ride the builder's own headers position: that position is filled once,
//! and a raw [`Headers`](ruststream::Headers) map stands for no contract, so a message declaring
//! `#[outgoing(headers = ..)]` has nowhere left to put a key. Stamping it on the publisher instead
//! keeps the position free for the contract.
//!
//! The core's publish chain is closed - [`Publish`](ruststream::runtime::Publish) keeps its fields
//! and constructors private, so a broker crate cannot add a position to it. A per-message argument
//! is embedded the other way round: by an adapter that sits *ahead* of the builder's entry point,
//! wrapping the publisher and editing each message on its way out. Because the adapter is itself a
//! [`Publisher`], the blanket [`PublishExt`](ruststream::runtime::PublishExt) hands it the whole
//! builder, so the key composes with every publish form without the chain having to open up.

use bytes::Bytes;
use ruststream::{OutgoingMessage, Publisher};

use crate::list::RedisListPublisher;
use crate::message::PARTITION_KEY_HEADER;
use crate::publisher::RedisPublisher;
use crate::pubsub::RedisPubSubPublisher;

/// A publisher adapter that stamps a partition key on every message sent through it.
///
/// Produced by [`RedisPublishExt::partition_key`]. It borrows the publisher it wraps and is a
/// [`Publisher`] in its own right, which is what gives it the whole core publish builder through
/// the blanket [`PublishExt`](ruststream::runtime::PublishExt): the key is applied as each message
/// leaves the builder, not as a position inside it.
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
#[derive(Debug, Clone, Copy)]
#[must_use = "a keyed publisher does nothing until something is published through it"]
pub struct PartitionKeyed<'a, P: ?Sized> {
    inner: &'a P,
    key: &'a [u8],
}

impl<P: ?Sized> PartitionKeyed<'_, P> {
    /// The partition key stamped on every message published through this handle.
    #[must_use]
    pub const fn key(&self) -> &[u8] {
        self.key
    }
}

impl<P: Publisher + ?Sized> Publisher for PartitionKeyed<'_, P> {
    type Error = P::Error;

    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        // `OutgoingMessage` exposes no mutable header access, so the key is added to a copy of the
        // map the builder just produced. `Headers` values are `Bytes` (refcounted), so the copy is
        // one small allocation per header name, not per payload byte.
        let mut headers = msg.headers().clone();
        headers.insert(PARTITION_KEY_HEADER, Bytes::copy_from_slice(self.key));
        let keyed = OutgoingMessage::new(msg.name(), msg.payload()).with_headers(headers);
        self.inner.publish(keyed).await
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
    /// Returns an adapter that stamps `key` as the partition key of everything sent through it.
    ///
    /// The key feeds the runtime's keyed worker lanes (`workers(n, by_key)`): deliveries sharing a
    /// key are dispatched to the same lane, so their relative order survives concurrency. It is
    /// carried as [`PARTITION_KEY_HEADER`] and replaces any value already under that name.
    fn partition_key<'a, K>(&'a self, key: &'a K) -> PartitionKeyed<'a, Self>
    where
        K: AsRef<[u8]> + ?Sized,
    {
        PartitionKeyed {
            inner: self,
            key: key.as_ref(),
        }
    }
}

// Bound to this crate's publishers rather than blanket over `Publisher`: the adapter is Redis
// vocabulary, and a blanket impl would grow it on every other broker's publisher too. Deliberately
// not implemented for `PartitionKeyed`, so re-keying an already-keyed publisher does not compile.
impl RedisPublishExt for RedisPublisher {}
impl RedisPublishExt for RedisListPublisher {}
impl RedisPublishExt for RedisPubSubPublisher {}

#[cfg(feature = "testing")]
impl RedisPublishExt for crate::testing::RedisTestPublisher {}
