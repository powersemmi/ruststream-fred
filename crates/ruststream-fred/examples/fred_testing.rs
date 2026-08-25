//! In-process unit-testing examples for ruststream-fred.
//!
//! The `testing` feature ships `RedisTestBroker`, an in-process transport whose connected form
//! implements `ruststream::testing::TestableBroker`. Use it to test `#[subscriber]` handlers the
//! same way you wire them in production: build a `RustStream` app around a `RedisTestBroker`, hand
//! it to the `TestApp` harness, and publish through the harness handle.
//!
//! This example is a test driver rather than a service, so it runs on a plain `#[tokio::main]`
//! instead of the `#[ruststream::app]` macro.
//!
//! ```text
//! cargo run --example fred_testing --features testing
//! ```

use std::sync::Arc;

// The harness surfaces stay explicit on both sides: neither prelude carries test tooling, because
// what a test drives is not what a service writes.
use ruststream::conformance::harness;
use ruststream::testing::TestApp;
use ruststream_fred::prelude::*;
use ruststream_fred::testing::RedisTestBroker;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
struct Payment {
    id: u64,
    user_id: u64,
    amount: u64,
}

// --8<-- [start:repository]
/// A repository connector. In production this would wrap a real database client;
/// the test uses the same connector with an in-memory store so the handler stays test-agnostic.
#[derive(Clone, Default)]
struct PaymentRepository {
    payments: Arc<Mutex<Vec<Payment>>>,
}

impl PaymentRepository {
    async fn save(&self, payment: Payment) {
        self.payments.lock().await.push(payment);
    }

    async fn count(&self) -> usize {
        self.payments.lock().await.len()
    }

    async fn contains(&self, id: u64) -> bool {
        self.payments.lock().await.iter().any(|p| p.id == id)
    }
}
// --8<-- [end:repository]

// --8<-- [start:business-handler]
/// A real production handler: validate the message, persist it, or drop it on validation failure.
#[subscriber(
    RedisStream::new("payments")
        .group("workers")
)]
async fn process_payment(
    payment: &Payment,
    ctx: &mut Context<'_, (), PaymentRepository>,
) -> HandlerResult {
    if payment.amount == 0 {
        // Invalid message: do not requeue, drop it.
        return HandlerResult::drop();
    }

    // The handler names its app state as the third `Context` generic; `ctx.state()` borrows the
    // typed `PaymentRepository` directly, with no lookup or downcast.
    ctx.state().save(payment.clone()).await;

    HandlerResult::ack()
}
// --8<-- [end:business-handler]

// --8<-- [start:stream-handler]
#[subscriber(
    RedisStream::new("events")
        .group("workers")
)]
async fn handle_stream_event(payment: &Payment) -> HandlerResult {
    println!("stream event {}", payment.id);
    HandlerResult::Ack
}
// --8<-- [end:stream-handler]

// --8<-- [start:list-handler]
#[subscriber(
    RedisList::new("jobs")
        .reliable()
)]
async fn handle_list_job(payment: &Payment) -> HandlerResult {
    println!("list job {}", payment.id);
    HandlerResult::Ack
}
// --8<-- [end:list-handler]

// --8<-- [start:pubsub-handler]
#[subscriber(RedisPubSub::new("notifications"))]
async fn handle_pubsub_notification(payment: &Payment) -> HandlerResult {
    println!("pubsub notification {}", payment.id);
    HandlerResult::Ack
}
// --8<-- [end:pubsub-handler]

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    test_payment_processing().await?;
    test_stream_delivery().await?;
    test_list_delivery().await?;
    test_pubsub_delivery().await?;
    test_conformance_suite().await?;

    Ok(())
}

fn payment(id: u64, amount: u64) -> Payment {
    Payment {
        id,
        user_id: 42,
        amount,
    }
}

async fn test_payment_processing() -> Result<(), Box<dyn std::error::Error>> {
    // --8<-- [start:business-test]
    let repository = PaymentRepository::default();
    let repository_for_app = repository.clone();

    let app = RustStream::new(AppInfo::new("test", "0.1.0"))
        // The startup hook produces the typed app state; the test keeps its own clone (the inner
        // store is shared via `Arc`) to assert on it afterwards.
        .on_startup(move |()| async move { Ok::<_, std::convert::Infallible>(repository_for_app) })
        .with_broker(RedisTestBroker::new(), |b| {
            b.include(process_payment);
        });

    // The harness runs the app's real startup and drives every publish to quiescence, so the
    // assertions below need no waiting.
    let tb = TestApp::start(app).await?;

    // The valid payment is saved; the invalid one (amount == 0) is dropped.
    tb.broker::<RedisTestBroker>()
        .publish("payments", &payment(1, 100))
        .await?;
    tb.broker::<RedisTestBroker>()
        .publish("payments", &payment(2, 0))
        .await?;

    assert!(repository.contains(1).await, "valid payment was not saved");
    assert!(
        !repository.contains(2).await,
        "invalid payment should have been dropped"
    );
    assert_eq!(repository.count().await, 1);

    tb.shutdown().await?;
    // --8<-- [end:business-test]
    Ok(())
}

async fn test_stream_delivery() -> Result<(), Box<dyn std::error::Error>> {
    // --8<-- [start:stream-test]
    let app =
        RustStream::new(AppInfo::new("test", "0.1.0")).with_broker(RedisTestBroker::new(), |b| {
            b.include(handle_stream_event);
        });

    let tb = TestApp::start(app).await?;
    tb.broker::<RedisTestBroker>()
        .publish("events", &payment(1, 100))
        .await?;

    tb.broker::<RedisTestBroker>()
        .subscriber("events")
        .assert_called_once()
        .settled(HandlerResult::Ack);

    tb.shutdown().await?;
    // --8<-- [end:stream-test]
    Ok(())
}

async fn test_list_delivery() -> Result<(), Box<dyn std::error::Error>> {
    // --8<-- [start:list-test]
    let app =
        RustStream::new(AppInfo::new("test", "0.1.0")).with_broker(RedisTestBroker::new(), |b| {
            b.include(handle_list_job);
        });

    let tb = TestApp::start(app).await?;
    tb.broker::<RedisTestBroker>()
        .publish("jobs", &payment(1, 100))
        .await?;

    tb.broker::<RedisTestBroker>()
        .subscriber("jobs")
        .assert_called_once()
        .settled(HandlerResult::Ack);

    tb.shutdown().await?;
    // --8<-- [end:list-test]
    Ok(())
}

async fn test_pubsub_delivery() -> Result<(), Box<dyn std::error::Error>> {
    // --8<-- [start:pubsub-test]
    let app =
        RustStream::new(AppInfo::new("test", "0.1.0")).with_broker(RedisTestBroker::new(), |b| {
            b.include(handle_pubsub_notification);
        });

    let tb = TestApp::start(app).await?;
    tb.broker::<RedisTestBroker>()
        .publish("notifications", &payment(1, 100))
        .await?;

    tb.broker::<RedisTestBroker>()
        .subscriber("notifications")
        .assert_called_once()
        .settled(HandlerResult::Ack);

    tb.shutdown().await?;
    // --8<-- [end:pubsub-test]
    Ok(())
}

async fn test_conformance_suite() -> Result<(), Box<dyn std::error::Error>> {
    // --8<-- [start:conformance]
    // The framework's conformance suite exercises routing, ack/nack, headers,
    // and requeue against the in-process test broker - no Redis server required.
    harness::run_suite(RedisTestBroker::new).await;
    // --8<-- [end:conformance]
    Ok(())
}

// --8<-- [start:unit-test]
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn valid_payment_is_saved_and_invalid_is_dropped() {
        let repository = PaymentRepository::default();
        let repository_for_app = repository.clone();

        let app = RustStream::new(AppInfo::new("test", "0.1.0"))
            .on_startup(
                move |()| async move { Ok::<_, std::convert::Infallible>(repository_for_app) },
            )
            .with_broker(RedisTestBroker::new(), |b| {
                b.include(process_payment);
            });

        let tb = TestApp::start(app).await.expect("startup failed");

        tb.broker::<RedisTestBroker>()
            .publish("payments", &payment(1, 100))
            .await
            .expect("publish valid");
        tb.broker::<RedisTestBroker>()
            .publish("payments", &payment(2, 0))
            .await
            .expect("publish invalid");

        assert!(repository.contains(1).await);
        assert!(!repository.contains(2).await);
        assert_eq!(repository.count().await, 1);

        tb.shutdown().await.expect("graceful shutdown failed");
    }
}
// --8<-- [end:unit-test]
