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
    AddressedCopies, Broker, ConnectedBroker, DescribeServer, OutgoingMessage, RawMessage,
    ServerSpec, Subscribe,
    testing::{Coordinator, TestableBroker},
};

use crate::{RedisListPublish, RedisPubSubPublish};
use crate::{
    error::RedisError,
    testing::{
        RedisTestPlainPublisher, RedisTestPublisher, RedisTestSubscriber,
        router::{Form, KeyRouter},
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
        self.open(
            key,
            Form::Stream,
            Settlement::Settleable,
            StreamRetry::default(),
        )
    }

    /// A [`RedisStream`](crate::RedisStream) subscription on `key`, retrying the way the
    /// descriptor's read mode and delay queue retry on a real server.
    ///
    /// A claiming subscription keeps a pending entries list of its own, so every delivery carries
    /// the two counters the server sends and reports its delivery count, a retried entry stays
    /// pending instead of being re-queued, and it is claimed back once it has been idle
    /// `min_idle`, ahead of fresh entries and with its delivery count one higher. A named delay
    /// queue makes a delay native, honoured exactly. A test moves either wait with
    /// [`TestApp::advance`](ruststream::testing::TestApp::advance).
    pub(crate) fn subscribe_stream(
        &self,
        key: impl Into<String>,
        retry: StreamRetry,
    ) -> impl Future<Output = Result<RedisTestSubscriber, RedisError>> {
        self.open(key, Form::Stream, Settlement::Settleable, retry)
    }

    /// A [`RedisList`](crate::RedisList) subscription on `key`: a reliable one settles, a simple
    /// one reports [`AckError::Unsupported`](ruststream::AckError::Unsupported) as it does on a
    /// server.
    pub(crate) fn subscribe_list(
        &self,
        key: impl Into<String>,
        reliable: bool,
    ) -> impl Future<Output = Result<RedisTestSubscriber, RedisError>> {
        let settlement = if reliable {
            Settlement::Settleable
        } else {
            Settlement::Unsupported
        };
        self.open(key, Form::List, settlement, StreamRetry::default())
    }

    /// A [`RedisPubSub`](crate::RedisPubSub) subscription on `channel`, whose deliveries report
    /// [`AckError::Unsupported`](ruststream::AckError::Unsupported) as they do on a server.
    pub(crate) fn subscribe_channel(
        &self,
        channel: impl Into<String>,
    ) -> impl Future<Output = Result<RedisTestSubscriber, RedisError>> {
        self.open(
            channel,
            Form::Channel,
            Settlement::Unsupported,
            StreamRetry::default(),
        )
    }

    fn open(
        &self,
        key: impl Into<String>,
        form: Form,
        settlement: Settlement,
        retry: StreamRetry,
    ) -> impl Future<Output = Result<RedisTestSubscriber, RedisError>> {
        if let Err(err) = self.state.alive() {
            return ready(Err(err));
        }
        let key = key.into();
        if let Err(err) = validate_key(&key) {
            return ready(Err(RedisError::Subscribe(err)));
        }
        let (id, requeue, rx) = self.state.router.subscribe(key, form);
        ready(Ok(RedisTestSubscriber::new(
            Arc::clone(&self.state),
            id,
            rx,
            requeue,
            settlement,
            retry,
        )))
    }

    /// Returns a publisher bound to this broker, carrying both transaction kinds like
    /// [`ConnectedRedisBroker::publisher`](crate::ConnectedRedisBroker::publisher). Cheap to clone.
    #[must_use]
    pub fn publisher(&self) -> RedisTestPublisher {
        RedisTestPublisher::new(Arc::clone(&self.state))
    }

    /// Returns a list publisher (`LPUSH`), the counterpart of
    /// [`ConnectedRedisBroker::list_publisher`](crate::ConnectedRedisBroker::list_publisher).
    /// Cheap to clone.
    ///
    /// It reaches a [`RedisList`](crate::RedisList) subscription and nothing else: a key a stream
    /// subscription reads, or a name a Pub/Sub subscription reads, refuses it as a server would.
    /// The policy's options are inert here, as its pairing notes.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::{Broker, OutgoingMessage, Publisher};
    /// use ruststream_fred::RedisListPublish;
    /// use ruststream_fred::testing::RedisTestBroker;
    ///
    /// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
    /// let connected = RedisTestBroker::new().connect().await?;
    /// let jobs = connected.list_publisher(RedisListPublish::new());
    /// jobs.publish(OutgoingMessage::new("jobs", b"{}".as_slice()), None).await?;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn list_publisher(&self, publish: RedisListPublish) -> RedisTestPlainPublisher {
        let _ = publish;
        RedisTestPlainPublisher::new(Arc::clone(&self.state), Form::List)
    }

    /// Returns a Pub/Sub publisher (`PUBLISH`), the counterpart of
    /// [`ConnectedRedisBroker::pubsub_publisher`](crate::ConnectedRedisBroker::pubsub_publisher).
    /// Cheap to clone.
    ///
    /// It reaches a [`RedisPubSub`](crate::RedisPubSub) subscription and nothing else: a name only
    /// a stream or a list subscription reads refuses it, since a channel publish never lands in a
    /// key. The policy's options are inert here, as its pairing notes.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::{Broker, OutgoingMessage, Publisher};
    /// use ruststream_fred::RedisPubSubPublish;
    /// use ruststream_fred::testing::RedisTestBroker;
    ///
    /// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
    /// let connected = RedisTestBroker::new().connect().await?;
    /// let events = connected.pubsub_publisher(RedisPubSubPublish::new());
    /// events.publish(OutgoingMessage::new("events", b"{}".as_slice()), None).await?;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn pubsub_publisher(&self, publish: RedisPubSubPublish) -> RedisTestPlainPublisher {
        let _ = publish;
        RedisTestPlainPublisher::new(Arc::clone(&self.state), Form::Channel)
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

/// How a stream subscription of the stand-in redelivers, mirroring what the descriptor asked the
/// real broker for.
///
/// Both answers are what the real subscription answers: a claiming read has Redis's own delayed
/// redelivery at `min_idle` granularity, and a named ZSET delay queue serves a delay exactly.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct StreamRetry {
    /// Set by [`RedisStream::claiming`](crate::RedisStream::claiming): a retried entry stays
    /// pending and is claimed back after this.
    min_idle: Option<Duration>,
    /// Set by [`RedisStream::delayed_retry`](crate::RedisStream::delayed_retry): a delay is
    /// honoured as asked.
    delayed: bool,
}

impl StreamRetry {
    /// A fresh-tail subscription, with a delay queue or without one.
    pub(crate) const fn fresh(delayed: bool) -> Self {
        Self {
            min_idle: None,
            delayed,
        }
    }

    /// A claiming subscription reading entries idle at least `min_idle`.
    pub(crate) const fn claiming(min_idle: Duration, delayed: bool) -> Self {
        Self {
            min_idle: Some(min_idle),
            delayed,
        }
    }

    pub(crate) const fn min_idle(self) -> Option<Duration> {
        self.min_idle
    }

    pub(crate) const fn is_delayed(self) -> bool {
        self.delayed
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
    /// The answer the real broker gives: the stand-in routes a publish to the subscription that
    /// opened under the same key, so the name is both ends.
    type Copies = AddressedCopies;

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
        // An injected message stands for no command of this crate, so it reaches whatever reads
        // the name and nothing refuses it.
        let _ = self.state.router.publish(
            message.name().to_owned(),
            Bytes::copy_from_slice(message.payload()),
            message.headers().clone(),
            self.state.coordinator().as_ref(),
            None,
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
