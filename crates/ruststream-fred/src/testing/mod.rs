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
//! * [`RedisTestPublisher`] / [`RedisTestPlainPublisher`] - what this crate's publish policies pair
//!   into here, one per capability surface the real publishers offer: the first carries both
//!   transaction kinds ([`RedisTestTransaction`] is the owned one), the second `Publisher` alone;
//! * [`RedisTestSubscriber`] / [`RedisTestMessage`] - `Subscriber` and `IncomingMessage` impls
//!   settling the way the form they were opened from settles: acknowledgement and
//!   `nack(requeue = true)` redelivery on a stream or a reliable list, `AckError::Unsupported` and
//!   no redelivery on Pub/Sub or a simple list.
//!
//! All three descriptors and all three publish policies mount here, so a service is tested on the
//! wiring it ships: the `#[subscriber(RedisStream::new(..).group(..))]` a routes file writes is the
//! one the harness mounts, the same holds for [`RedisList`](crate::RedisList) and
//! [`RedisPubSub`](crate::RedisPubSub), and `.out(Reply, Publish)` names the production policy on
//! both brokers. There is no test-only policy type; [`RedisPublish`](crate::RedisPublish) is also
//! the stand-in's default reply publisher.
//!
//! A descriptor resolves to its key or channel, which is all the stand-in routes by, and a policy
//! pairs into the publisher whose capability surface matches the one it would get on a real server,
//! so a slot bound here is a slot that compiles in production. The `SubscriptionSource` and
//! `PublishPolicy` impls for [`ConnectedRedisTestBroker`] carry the per-form detail: what the mount
//! ignores, and which configurations it refuses at startup rather than reinterprets.
//!
//! What the stand-in owes is not left to those notes alone. The framework's contract suites run
//! against it as well as against a real server (`tests/conformance_fred.rs`): the routing suite,
//! `harness::lifecycle` with its post-shutdown publish, `capabilities::batches` on all three forms,
//! and both transaction suites. Only `capabilities::seeking` is live-only, since the stand-in keeps
//! no consumer-group cursor to reposition.
//!
//! No `redis-server`, no docker, no network. Broker-specific edge cases (consumer-group cursors,
//! `XAUTOCLAIM` redelivery, idle reclaim, `MAXLEN` trimming, dead-letter routing) are out of scope
//! here. Exercise them against a real Redis server.

mod broker;
mod publisher;
mod router;
mod subscriber;

pub use broker::{ConnectedRedisTestBroker, RedisTestBroker};
pub use publisher::{RedisTestPlainPublisher, RedisTestPublisher, RedisTestTransaction};
pub use subscriber::{RedisTestMessage, RedisTestSubscriber};
