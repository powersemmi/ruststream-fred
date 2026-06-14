//! [`RedisTestClient`]: `TestClient` driver consumed by the conformance harness.

use std::time::Duration;

use ruststream::{Broker, OutgoingMessage, Publisher, RawMessage, testing::TestClient};

use crate::{
    error::RedisError,
    testing::{
        broker::RedisTestBroker, publisher::RedisTestPublisher, subscriber::RedisTestSubscriber,
    },
};

/// Driver around a single [`RedisTestBroker`] instance.
///
/// `RedisTestClient::start()` constructs a fresh, isolated broker. Use it as the entry point in the
/// `ruststream::conformance` harness and in handler integration tests.
pub struct RedisTestClient {
    broker: RedisTestBroker,
}

impl std::fmt::Debug for RedisTestClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisTestClient")
            .field("broker", &self.broker)
            .finish()
    }
}

impl TestClient for RedisTestClient {
    type Broker = RedisTestBroker;
    type Subscriber = RedisTestSubscriber;
    type Publisher = RedisTestPublisher;
    type Error = RedisError;

    async fn start() -> Result<Self, Self::Error> {
        Ok(Self {
            broker: RedisTestBroker::new(),
        })
    }

    fn broker(&self) -> &Self::Broker {
        &self.broker
    }

    async fn publish(&self, topic: &str, payload: &[u8]) -> Result<(), Self::Error> {
        let publisher = self.broker.publisher();
        publisher
            .publish(OutgoingMessage::new(topic, payload))
            .await
    }

    async fn subscribe(&self, topic: &str) -> Result<RedisTestSubscriber, Self::Error> {
        self.broker.subscribe(topic).await
    }

    async fn publisher(&self) -> Result<Self::Publisher, Self::Error> {
        Ok(self.broker.publisher())
    }

    async fn expect_published(
        &self,
        topic: &str,
        count: usize,
        timeout_dur: Duration,
    ) -> Result<Vec<RawMessage>, Self::Error> {
        Ok(self
            .broker
            .state()
            .router
            .expect_published(topic, count, timeout_dur)
            .await)
    }

    async fn shutdown(self) -> Result<(), Self::Error> {
        <RedisTestBroker as Broker>::shutdown(&self.broker).await
    }
}
