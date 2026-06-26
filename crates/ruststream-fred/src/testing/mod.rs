//! In-process Redis test transport used by application unit tests and the conformance suite.
//!
//! Gated by the `testing` cargo feature. The broker is a synchronous dispatcher: `publish` fans the
//! message out to every subscriber whose stream key matches exactly. Public surface:
//!
//! * [`RedisTestBroker`] - a full `Broker` + `Subscribe` + `DescribeServer` backed by an in-process
//!   key router, which also implements [`ruststream::testing::TestableBroker`] so it plugs straight
//!   into the [`TestApp`](ruststream::testing::TestApp) harness and
//!   [`conformance::harness::run_suite`](ruststream::conformance::harness::run_suite);
//! * [`RedisTestPublisher`] - `Publisher`;
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

pub use broker::RedisTestBroker;
pub use publisher::RedisTestPublisher;
pub use subscriber::{RedisTestMessage, RedisTestSubscriber};
