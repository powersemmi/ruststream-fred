//! Redis / Valkey broker implementation for `RustStream`, backed by [`fred`].
//!
//! This crate implements the `RustStream` broker contract over Redis Streams: durable consumer
//! groups with acknowledgement, redelivery, and crash recovery. Subjects are stream keys; a
//! subscription reads through a consumer group, either off the fresh tail
//! ([`RedisStream::new`]) or reclaiming another consumer's stale pending entries
//! ([`RedisStream::reclaim`]).
//!
//! Settlement follows the republish-retry model: `ack` is `XACK`, `nack(requeue = true)` re-appends
//! a copy to the same stream then acks the original, and `nack(requeue = false)` acks to drop.
//!
//! The lifecycle is the framework's ladder of consuming transitions: [`RedisBroker`] records the
//! topology synchronously, [`Broker::connect`](ruststream::Broker::connect) yields the
//! [`ConnectedRedisBroker`] that every subscription and publisher is reached from, and
//! [`ConnectedBroker::shutdown`](ruststream::ConnectedBroker::shutdown) yields the terminal
//! [`ClosedRedisBroker`]. Publishers are declared as a policy ([`RedisPublish`],
//! [`RedisPubSubPublish`], [`RedisListPublish`]) that pairs with the connected form.
//!
//! [`fred`]: https://docs.rs/fred

#![forbid(unsafe_code)]

mod broker;
mod convert;
mod deadletter;
mod delay;
mod envelope;
mod error;
mod list;
mod message;
mod partition;
mod publisher;
mod pubsub;
mod recovery;
mod seek;
mod stream;
mod subscriber;

// The module carries its own `//!` summary. A second doc fragment here would be written in the
// crate root's scope, and rustdoc would then resolve the module's intra-doc links there too.
pub mod context;

pub use broker::{ClosedRedisBroker, ConnectedRedisBroker, RedisBroker};
pub use deadletter::{DEAD_LETTER_REASON_HEADER, DELIVERY_COUNT_HEADER, IDLE_MS_HEADER};
pub use delay::DelayedRetry;
pub use error::RedisError;
pub use list::{
    RedisList, RedisListMessage, RedisListPublish, RedisListPublisher, RedisListSubscriber,
};
pub use message::{PARTITION_KEY_HEADER, RedisMessage};
pub use partition::{PartitionKeyed, RedisPublishExt};
pub use publisher::{RedisPublish, RedisPublisher, RedisTransaction};
pub use pubsub::{
    PubSubMode, RedisPubSub, RedisPubSubMessage, RedisPubSubPublish, RedisPubSubPublisher,
    RedisPubSubSubscriber,
};
pub use seek::{EntryId, RedisGroupPosition, RedisGroupSeeker};
pub use stream::{RedisStream, StreamStart};
pub use subscriber::RedisSubscriber;

// fred auth/TLS types re-exported for the `RedisBroker::tls` / `::credential_provider` builders, so
// callers need not depend on `fred` directly to name them.
#[cfg(feature = "credential-provider")]
pub use fred::types::config::CredentialProvider;
#[cfg(any(
    feature = "tls-rustls",
    feature = "tls-rustls-ring",
    feature = "tls-native-tls"
))]
pub use fred::types::config::{TlsConfig, TlsConnector};

#[cfg(feature = "testing")]
pub mod testing;
