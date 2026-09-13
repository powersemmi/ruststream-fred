//! The in-process broker ladder: [`RedisTestBroker`] -> [`ConnectedRedisTestBroker`].
//!
//! The connected form implements [`TestableBroker`](ruststream::testing::TestableBroker), so the
//! same transport drives the [`TestApp`](ruststream::testing::TestApp) harness and the framework's
//! conformance suite.

use std::future::{Future, ready};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use bytes::Bytes;
use ruststream::{
    Broker, ConnectedBroker, DescribeServer, OutgoingMessage, RawMessage, RedeliveryAddress,
    ServerSpec, Subscribe,
    testing::{Coordinator, TestableBroker},
};

use crate::{
    error::RedisError,
    testing::{
        RedisTestPlainPublisher, RedisTestPublisher, RedisTestSubscriber, router::KeyRouter,
    },
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
    /// Set once the connected form is shut down. Handles that alias the connection outlive it, so
    /// the flag is what makes them refuse, the way the real broker's dropped pool does.
    closed: AtomicBool,
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

    fn close(&self) {
        self.closed.store(true, Ordering::Release);
    }

    /// Refuses use of a handle that outlived the connection, mirroring
    /// [`RedisError::ShutDown`] from the real broker's dropped pool.
    ///
    /// # Errors
    ///
    /// Returns [`RedisError::ShutDown`] once the connected form has been shut down.
    pub(crate) fn alive(&self) -> Result<(), RedisError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(RedisError::ShutDown);
        }
        Ok(())
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
/// message to every matching subscriber's channel. Settlement follows the form the subscription was
/// opened from: on a stream or a reliable list `ack` / `nack(requeue = false)` consume the delivery
/// and `nack(requeue = true)` re-sends it to the same subscriber's queue, while Pub/Sub and a simple
/// list report [`AckError::Unsupported`](ruststream::AckError::Unsupported) and redeliver nothing,
/// as they do on a real server. After [`ConnectedBroker::shutdown`] a publisher or subscription that
/// aliases the closed connection reports [`RedisError::ShutDown`].
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
    /// Returns [`RedisError::Subscribe`] when `key` is empty, or [`RedisError::ShutDown`] once the
    /// connection has been shut down.
    // Awaited like `ConnectedRedisBroker::subscribe`; the form differs only because this body is
    // synchronous, so there is nothing to suspend on.
    pub fn subscribe(
        &self,
        key: impl Into<String>,
    ) -> impl Future<Output = Result<RedisTestSubscriber, RedisError>> {
        self.open(key, Settlement::Settleable, None)
    }

    /// A [`RedisStream::claiming`](crate::RedisStream::claiming) subscription on `key`.
    ///
    /// It keeps a pending entries list of its own, so every delivery carries the two counters the
    /// server sends, a retried entry stays pending instead of being re-queued, and it is claimed
    /// back once it has been idle `min_idle`, ahead of fresh entries and with its delivery count
    /// one higher. A test moves that wait with
    /// [`TestApp::advance`](ruststream::testing::TestApp::advance).
    pub(crate) fn subscribe_claiming(
        &self,
        key: impl Into<String>,
        min_idle: Duration,
    ) -> impl Future<Output = Result<RedisTestSubscriber, RedisError>> {
        self.open(key, Settlement::Settleable, Some(min_idle))
    }

    /// The same subscription with settlement refused, for the forms whose real deliveries report
    /// [`AckError::Unsupported`](ruststream::AckError::Unsupported): Pub/Sub and a simple list.
    pub(crate) fn subscribe_unsettleable(
        &self,
        key: impl Into<String>,
    ) -> impl Future<Output = Result<RedisTestSubscriber, RedisError>> {
        self.open(key, Settlement::Unsupported, None)
    }

    fn open(
        &self,
        key: impl Into<String>,
        settlement: Settlement,
        min_idle: Option<Duration>,
    ) -> impl Future<Output = Result<RedisTestSubscriber, RedisError>> {
        if let Err(err) = self.state.alive() {
            return ready(Err(err));
        }
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
            settlement,
            min_idle,
        )))
    }

    /// Returns a publisher bound to this broker, carrying both transaction kinds like
    /// [`ConnectedRedisBroker::publisher`](crate::ConnectedRedisBroker::publisher). Cheap to clone.
    #[must_use]
    pub fn publisher(&self) -> RedisTestPublisher {
        RedisTestPublisher::new(Arc::clone(&self.state))
    }

    /// Returns a publisher offering [`Publisher`](ruststream::Publisher) alone, the surface the
    /// list and Pub/Sub publishers have on a real server. Cheap to clone.
    ///
    /// Reached in a test by pairing [`RedisListPublish`](crate::RedisListPublish) or
    /// [`RedisPubSubPublish`](crate::RedisPubSubPublish), which is how a routes file writes it;
    /// this is the direct handle for code that has no policy in hand.
    #[must_use]
    pub fn plain_publisher(&self) -> RedisTestPlainPublisher {
        RedisTestPlainPublisher::new(Arc::clone(&self.state))
    }
}

impl ConnectedBroker for ConnectedRedisTestBroker {
    type Error = RedisError;
    type Closed = ();

    fn shutdown(self) -> impl Future<Output = Result<Self::Closed, Self::Error>> {
        // Closing before clearing, so a publisher racing the teardown is refused rather than
        // writing into a router nobody is subscribed to any more.
        self.state.close();
        self.state.router.clear();
        ready(Ok(()))
    }
}

/// Whether a subscription's deliveries can be settled.
///
/// The forms differ on a real server: a stream and a reliable list acknowledge, while Pub/Sub and
/// a simple list report [`AckError::Unsupported`](ruststream::AckError::Unsupported). The stand-in
/// carries the same split, so a test cannot settle what the transport it stands in for cannot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Settlement {
    /// `ack` and `nack` take effect, as on a stream or a reliable list.
    Settleable,
    /// `ack` and `nack` report `Unsupported`, as on Pub/Sub or a simple list.
    Unsupported,
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

    /// The key itself, the answer the real broker gives: the stand-in routes a publish to the
    /// subscription that opened under the same key.
    fn redelivery_address(&self, name: &str) -> Option<RedeliveryAddress> {
        Some(RedeliveryAddress::new(name.to_owned()))
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
