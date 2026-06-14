//! [`RedisTestBroker`]: `Broker` implementation backed by the in-process handler-stub dispatcher.

use std::{sync::Arc, time::Duration};

use ruststream::{Broker, DescribeServer, RawMessage, ServerSpec, Subscribe};

use crate::{
    error::RedisError,
    testing::{RedisTestPublisher, RedisTestSubscriber, router::KeyRouter},
};

/// Shared state owned by every clone of a single test broker instance.
///
/// Cloning [`RedisTestBroker`] clones an [`Arc`] of this; all clones see the same router and
/// therefore the same set of subscriptions. Distinct instances (different [`RedisTestBroker::new`]
/// calls) are fully isolated.
#[derive(Default)]
pub(crate) struct TestBrokerState {
    pub(crate) router: KeyRouter,
}

impl std::fmt::Debug for TestBrokerState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestBrokerState")
            .field("router", &self.router)
            .finish()
    }
}

/// In-process Redis broker used for handler-level tests.
///
/// `publish` matches stream keys exactly (Redis Streams have no wildcard subjects) and hands the
/// message to every matching subscriber's channel; `ack`/`nack(requeue = false)` consume the
/// delivery and `nack(requeue = true)` re-sends it to the same subscriber's queue.
///
/// Broker-specific edge cases (consumer-group cursors, `XAUTOCLAIM` redelivery, idle reclaim,
/// `MAXLEN` trimming, dead-letter routing) are intentionally NOT simulated. Use a real Redis server
/// for those scenarios.
#[derive(Clone, Default, Debug)]
pub struct RedisTestBroker {
    state: Arc<TestBrokerState>,
}

impl RedisTestBroker {
    /// Constructs a fresh, isolated test broker. Equivalent to [`Self::default`].
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn state(&self) -> &Arc<TestBrokerState> {
        &self.state
    }

    /// Opens a subscription on the stream `key`. Mirrors the public surface of
    /// [`crate::RedisBroker::subscribe`]; in handler-stub mode only the key is used for routing
    /// (no consumer-group bookkeeping).
    ///
    /// # Errors
    ///
    /// Returns [`RedisError::Subscribe`] when `key` is empty.
    #[allow(
        clippy::unused_async,
        reason = "API parity with RedisBroker::subscribe"
    )]
    pub async fn subscribe(
        &self,
        key: impl Into<String>,
    ) -> Result<RedisTestSubscriber, RedisError> {
        let key = key.into();
        validate_key(&key).map_err(RedisError::Subscribe)?;
        let (id, requeue, rx) = self.state.router.subscribe(key);
        Ok(RedisTestSubscriber::new(
            Arc::clone(&self.state),
            id,
            rx,
            requeue,
        ))
    }

    /// Returns a publisher bound to this broker. Cheap to clone.
    #[must_use]
    pub fn publisher(&self) -> RedisTestPublisher {
        RedisTestPublisher::new(Arc::clone(&self.state))
    }

    /// Awaits until `count` messages have landed on `key` (or the timeout elapses) and returns the
    /// recorded prefix of the published log. Returns whatever is recorded on timeout, never
    /// blocking past it.
    pub async fn expect_published(
        &self,
        key: &str,
        count: usize,
        timeout_dur: Duration,
    ) -> Vec<RawMessage> {
        self.state
            .router
            .expect_published(key, count, timeout_dur)
            .await
    }
}

impl Broker for RedisTestBroker {
    type Error = RedisError;

    async fn connect(&self) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), Self::Error> {
        self.state.router.clear();
        Ok(())
    }
}

#[allow(clippy::use_self)]
impl Subscribe for RedisTestBroker {
    type Subscriber = RedisTestSubscriber;

    async fn subscribe(&self, name: &str) -> Result<Self::Subscriber, Self::Error> {
        RedisTestBroker::subscribe(self, name).await
    }
}

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
        // The in-process broker has no real server; report a well-known in-memory address.
        ServerSpec::new("in-process", "redis")
    }
}
