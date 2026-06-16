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
mod publisher;
mod pubsub;
mod recovery;
mod stream;
mod subscriber;

pub use broker::RedisBroker;
pub use deadletter::{DEAD_LETTER_REASON_HEADER, DELIVERY_COUNT_HEADER, IDLE_MS_HEADER};
pub use delay::DelayedRetry;
pub use error::RedisError;
pub use list::{RedisList, RedisListMessage, RedisListPublisher, RedisListSubscriber};
pub use message::{PARTITION_KEY_HEADER, RedisMessage};
pub use publisher::RedisPublisher;
pub use pubsub::{
    PubSubMode, RedisPubSub, RedisPubSubMessage, RedisPubSubPublisher, RedisPubSubSubscriber,
};
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
