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
mod envelope;
mod error;
mod list;
mod message;
mod publisher;
mod pubsub;
mod stream;
mod subscriber;

pub use broker::RedisBroker;
pub use error::RedisError;
pub use list::{RedisList, RedisListMessage, RedisListPublisher, RedisListSubscriber};
pub use message::{PARTITION_KEY_HEADER, RedisMessage};
pub use publisher::RedisPublisher;
pub use pubsub::{
    PubSubMode, RedisPubSub, RedisPubSubMessage, RedisPubSubPublisher, RedisPubSubSubscriber,
};
pub use stream::{RedisStream, StreamStart};
pub use subscriber::RedisSubscriber;

#[cfg(feature = "testing")]
pub mod testing;
