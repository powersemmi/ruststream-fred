//! The in-process broker ladder: [`RedisTestBroker`] -> [`ConnectedRedisTestBroker`].
//!
//! The connected form implements [`TestableBroker`](ruststream::testing::TestableBroker), so the
//! same transport drives the [`TestApp`](ruststream::testing::TestApp) harness and the framework's
//! conformance suite.

use std::future::{Future, ready};
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use ruststream::{
    Broker, ConnectedBroker, DescribeServer, OutgoingMessage, RawMessage, ServerSpec, Subscribe,
    testing::{Coordinator, TestableBroker},
};

use crate::{
    error::RedisError,
    testing::{RedisTestPublisher, RedisTestSubscriber, router::KeyRouter},
};

/// Shared state owned by every clone of a single test broker instance.
///
/// Cloning [`RedisTestBroker`] clones an [`Arc`] of this; all clones and the connected form see the
/// same router and therefore the same set of subscriptions. Distinct instances (different
/// [`RedisTestBroker::new`] calls) are fully isolated.
#[derive(Default)]
pub(crate) struct TestBrokerState {
    pub(crate) router: KeyRouter,
    /// The harness's quiescence-and-recording coordinator, installed by a
    /// [`TestApp`](ruststream::testing::TestApp) run. Empty in production and under the conformance
    /// suite, so fanout does no extra work.
    coordinator: OnceLock<Coordinator>,
}

impl TestBrokerState {
    /// Installs the harness coordinator for a [`TestApp`](ruststream::testing::TestApp) run.
    /// Idempotent: a second install on the same broker is ignored.
    pub(crate) fn install_coordinator(&self, coordinator: Coordinator) {
        let _ = self.coordinator.set(coordinator);
    }

    /// A clone of the installed coordinator, threaded into each subscriber, delivery, and publish so
    /// a requeue can re-count and a consumed delivery can decrement. `None` outside a harness run.
    pub(crate) fn coordinator(&self) -> Option<Coordinator> {
        self.coordinator.get().cloned()
    }
}

impl std::fmt::Debug for TestBrokerState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestBrokerState")
            .field("router", &self.router)
            .finish_non_exhaustive()
    }
}

/// In-process stand-in for [`RedisBroker`](crate::RedisBroker), used for handler-level tests.
///
/// `new` is synchronous and I/O-free like the real one, and [`Broker::connect`] yields the
/// [`ConnectedRedisTestBroker`] the subscriptions and publishers hang off.
///
/// `publish` matches stream keys exactly (Redis Streams have no wildcard subjects) and hands the
/// message to every matching subscriber's channel; `ack`/`nack(requeue = false)` consume the
/// delivery and `nack(requeue = true)` re-sends it to the same subscriber's queue.
///
/// Broker-specific edge cases (consumer-group cursors, `XAUTOCLAIM` redelivery, idle reclaim,
/// `MAXLEN` trimming, dead-letter routing) are intentionally NOT simulated. Use a real Redis server
/// for those scenarios.
///
/// # Examples
///
/// ```
/// use ruststream_fred::testing::RedisTestBroker;
///
/// let broker = RedisTestBroker::new();
/// # let _ = broker;
/// ```
#[derive(Clone, Default, Debug)]
#[must_use]
pub struct RedisTestBroker {
    state: Arc<TestBrokerState>,
}

impl RedisTestBroker {
    /// Constructs a fresh, isolated test broker. Equivalent to [`Self::default`].
    pub fn new() -> Self {
        Self::default()
    }
}

impl Broker for RedisTestBroker {
    type Error = RedisError;
    type Connected = ConnectedRedisTestBroker;

    fn connect(self) -> impl Future<Output = Result<Self::Connected, Self::Error>> {
        ready(Ok(ConnectedRedisTestBroker { state: self.state }))
    }
}

/// The connected form of [`RedisTestBroker`]: what the harness and the conformance suite drive.
#[derive(Clone, Debug)]
pub struct ConnectedRedisTestBroker {
    state: Arc<TestBrokerState>,
}

impl ConnectedRedisTestBroker {
    /// Opens a subscription on the stream `key`. Mirrors the public surface of
    /// [`ConnectedRedisBroker::subscribe`](crate::ConnectedRedisBroker::subscribe); in
    /// handler-stub mode only the key is used for routing (no consumer-group bookkeeping).
    ///
    /// # Errors
    ///
    /// Returns [`RedisError::Subscribe`] when `key` is empty.
    // Awaited like `ConnectedRedisBroker::subscribe`; the form differs only because this body is
    // synchronous, so there is nothing to suspend on.
    pub fn subscribe(
        &self,
        key: impl Into<String>,
    ) -> impl Future<Output = Result<RedisTestSubscriber, RedisError>> {
        let key = key.into();
        if let Err(err) = validate_key(&key) {
            return ready(Err(RedisError::Subscribe(err)));
        }
        let (id, requeue, rx) = self.state.router.subscribe(key);
        ready(Ok(RedisTestSubscriber::new(
            Arc::clone(&self.state),
            id,
            rx,
            requeue,
        )))
    }

    /// Returns a publisher bound to this broker. Cheap to clone.
    #[must_use]
    pub fn publisher(&self) -> RedisTestPublisher {
        RedisTestPublisher::new(Arc::clone(&self.state))
    }
}

impl ConnectedBroker for ConnectedRedisTestBroker {
    type Error = RedisError;
    type Closed = ();

    fn shutdown(self) -> impl Future<Output = Result<Self::Closed, Self::Error>> {
        self.state.router.clear();
        ready(Ok(()))
    }
}

#[allow(
    clippy::use_self,
    reason = "the type name disambiguates the inherent subscribe from this trait method"
)]
impl Subscribe for ConnectedRedisTestBroker {
    type Subscriber = RedisTestSubscriber;

    async fn subscribe(&self, name: &str) -> Result<Self::Subscriber, Self::Error> {
        ConnectedRedisTestBroker::subscribe(self, name).await
    }
}

// --8<-- [start:testable]
impl TestableBroker for ConnectedRedisTestBroker {
    fn install_coordinator(&self, coordinator: Coordinator) {
        self.state.install_coordinator(coordinator);
    }

    fn inject(&self, message: OutgoingMessage<'_>) {
        // Route synchronously through the broker's own fanout, bypassing subject validation: the
        // harness injects as an external producer would, and the publish is recorded and counted
        // like any other.
        self.state.router.publish(
            message.name().to_owned(),
            Bytes::copy_from_slice(message.payload()),
            message.headers().clone(),
            self.state.coordinator().as_ref(),
        );
    }

    fn published(&self, name: &str) -> Vec<RawMessage> {
        self.state.router.published(name)
    }
}

ruststream::register_testable_broker!(ConnectedRedisTestBroker);
// --8<-- [end:testable]

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Validates that `key` is a usable stream key (non-empty).
fn validate_key(key: &str) -> Result<(), BoxError> {
    if key.is_empty() {
        return Err("stream key must be non-empty".into());
    }
    Ok(())
}

/// Validates that `key` is publishable, converting a failure into [`RedisError::Publish`].
pub(crate) fn validate_publish_key(key: &str) -> Result<(), RedisError> {
    validate_key(key).map_err(RedisError::Publish)
}

impl DescribeServer for RedisTestBroker {
    fn describe_server(&self) -> ServerSpec {
        // The in-process broker has no real server; report itself as in-process over `redis`.
        ServerSpec::in_process("redis")
    }
}
