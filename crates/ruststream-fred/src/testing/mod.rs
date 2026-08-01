//! In-process Redis test transport used by application unit tests and the conformance suite.
//!
//! Gated by the `testing` cargo feature. The broker is a synchronous dispatcher: `publish` fans the
//! message out to every subscriber whose stream key matches exactly. Public surface:
//!
//! * [`RedisTestBroker`] - the unconnected form, a `Broker` + `DescribeServer` built synchronously
//!   like the real one;
//! * [`ConnectedRedisTestBroker`] - its connected form, backed by an in-process key router, which
//!   implements `Subscribe` and [`ruststream::testing::TestableBroker`] so it plugs straight into
//!   the [`TestApp`](ruststream::testing::TestApp) harness and the framework's conformance suite;
//! * [`RedisTestPublish`] / [`RedisTestPublisher`] - the publish policy and the live publisher it
//!   pairs into;
//! * [`RedisTestSubscriber`] / [`RedisTestMessage`] - `Subscriber` and `IncomingMessage` impls with
//!   `nack(requeue = true)` redelivery (re-sent into the same subscriber's queue).
//!
//! No `redis-server`, no docker, no network. Broker-specific edge cases (consumer-group cursors,
//! `XAUTOCLAIM` redelivery, idle reclaim, `MAXLEN` trimming, dead-letter routing) are out of scope
//! here. Exercise them against a real Redis server.

mod broker;
mod publisher;
mod router;
mod subscriber;

pub use broker::{ConnectedRedisTestBroker, RedisTestBroker};
pub use publisher::{RedisTestPublish, RedisTestPublisher};
pub use subscriber::{RedisTestMessage, RedisTestSubscriber};
