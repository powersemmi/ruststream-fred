//! In-process Redis test driver used by handler integration tests.
//!
//! Gated by the `testing` cargo feature. The broker is a synchronous dispatcher: `publish` fans the
//! message out to every subscriber whose stream key matches exactly. Public surface:
//!
//! * [`RedisTestBroker`] - `Broker` impl backed by an in-process key router;
//! * [`RedisTestPublisher`] - `Publisher`;
//! * [`RedisTestSubscriber`] / [`RedisTestMessage`] - `Subscriber` and `IncomingMessage` impls with
//!   `nack(requeue = true)` redelivery (re-sent into the same subscriber's queue);
//! * [`RedisTestClient`] - `TestClient` driver consumed by the conformance harness.
//!
//! No `redis-server`, no docker, no network. Broker-specific edge cases (consumer-group cursors,
//! `XAUTOCLAIM` redelivery, idle reclaim, `MAXLEN` trimming, dead-letter routing) are out of scope
//! here. Exercise them against a real Redis server.

mod broker;
mod client;
mod publisher;
mod router;
mod subscriber;

pub use broker::RedisTestBroker;
pub use client::RedisTestClient;
pub use publisher::RedisTestPublisher;
pub use subscriber::{RedisTestMessage, RedisTestSubscriber};
