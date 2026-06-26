//! In-process unit-testing examples for ruststream-fred.
//!
//! The `testing` feature ships `RedisTestBroker`, an in-process transport that is a full broker and
//! also implements `ruststream::testing::TestableBroker`. Use it to test `#[subscriber]` handlers
//! the same way you wire them in production: build a `RustStream` app around a `RedisTestBroker`,
//! then drive publishes by injecting messages onto the broker's bus.
//!
//! ```text
//! cargo run --example fred_testing --features testing
//! ```

use std::sync::Arc;
use std::time::Duration;

use ruststream::OutgoingMessage;
use ruststream::conformance::harness;
use ruststream::runtime::{AppInfo, HandlerResult, RustStream};
use ruststream::subscriber;
use ruststream::testing::TestableBroker;
use ruststream_fred::{RedisList, RedisPubSub, RedisStream, testing::RedisTestBroker};
use serde::Deserialize;
use tokio::sync::Mutex;

#[derive(Debug, Deserialize, Clone, PartialEq)]
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

async fn test_payment_processing() -> Result<(), Box<dyn std::error::Error>> {
    // --8<-- [start:business-test]
    let broker = RedisTestBroker::new();
    let repository = PaymentRepository::default();
    let repository_for_app = repository.clone();

    let app = RustStream::new(AppInfo::new("test", "0.1.0"))
        // The startup hook produces the typed app state; the test keeps its own clone (the inner
        // store is shared via `Arc`) to assert on it afterwards.
        .on_startup(move |()| async move { Ok::<_, std::convert::Infallible>(repository_for_app) })
        .with_broker(broker.clone(), |b| {
            b.include(process_payment);
        });

    let task = tokio::spawn(async move {
        app.run_until(tokio::time::sleep(Duration::from_millis(500)))
            .await
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Valid payment is saved.
    broker.inject(OutgoingMessage::new(
        "payments",
        br#"{"id":1,"user_id":42,"amount":100}"#,
    ));
    // Invalid payment (amount == 0) is dropped.
    broker.inject(OutgoingMessage::new(
        "payments",
        br#"{"id":2,"user_id":42,"amount":0}"#,
    ));

    // Wait until the valid payment is persisted.
    let deadline = Duration::from_secs(2);
    let start = std::time::Instant::now();
    while !repository.contains(1).await && start.elapsed() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert!(repository.contains(1).await, "valid payment was not saved");
    assert!(
        !repository.contains(2).await,
        "invalid payment should have been dropped"
    );
    assert_eq!(repository.count().await, 1);

    task.await??;
    // --8<-- [end:business-test]
    Ok(())
}

async fn test_stream_delivery() -> Result<(), Box<dyn std::error::Error>> {
    // --8<-- [start:stream-test]
    let broker = RedisTestBroker::new();

    let app = RustStream::new(AppInfo::new("test", "0.1.0")).with_broker(broker.clone(), |b| {
        b.include(handle_stream_event);
    });

    let task = tokio::spawn(async move {
        app.run_until(tokio::time::sleep(Duration::from_millis(500)))
            .await
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    broker.inject(OutgoingMessage::new(
        "events",
        br#"{"id":1,"user_id":42,"amount":100}"#,
    ));
    task.await??;
    // --8<-- [end:stream-test]
    Ok(())
}

async fn test_list_delivery() -> Result<(), Box<dyn std::error::Error>> {
    // --8<-- [start:list-test]
    let broker = RedisTestBroker::new();

    let app = RustStream::new(AppInfo::new("test", "0.1.0")).with_broker(broker.clone(), |b| {
        b.include(handle_list_job);
    });

    let task = tokio::spawn(async move {
        app.run_until(tokio::time::sleep(Duration::from_millis(500)))
            .await
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    broker.inject(OutgoingMessage::new(
        "jobs",
        br#"{"id":1,"user_id":42,"amount":100}"#,
    ));
    task.await??;
    // --8<-- [end:list-test]
    Ok(())
}

async fn test_pubsub_delivery() -> Result<(), Box<dyn std::error::Error>> {
    // --8<-- [start:pubsub-test]
    let broker = RedisTestBroker::new();

    let app = RustStream::new(AppInfo::new("test", "0.1.0")).with_broker(broker.clone(), |b| {
        b.include(handle_pubsub_notification);
    });

    let task = tokio::spawn(async move {
        app.run_until(tokio::time::sleep(Duration::from_millis(500)))
            .await
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    broker.inject(OutgoingMessage::new(
        "notifications",
        br#"{"id":1,"user_id":42,"amount":100}"#,
    ));
    task.await??;
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
        let broker = RedisTestBroker::new();
        let repository = PaymentRepository::default();
        let repository_for_app = repository.clone();

        let app = RustStream::new(AppInfo::new("test", "0.1.0"))
            .on_startup(
                move |()| async move { Ok::<_, std::convert::Infallible>(repository_for_app) },
            )
            .with_broker(broker.clone(), |b| {
                b.include(process_payment);
            });

        let task = tokio::spawn(async move {
            app.run_until(tokio::time::sleep(Duration::from_millis(500)))
                .await
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        broker.inject(OutgoingMessage::new(
            "payments",
            br#"{"id":1,"user_id":42,"amount":100}"#,
        ));
        broker.inject(OutgoingMessage::new(
            "payments",
            br#"{"id":2,"user_id":42,"amount":0}"#,
        ));

        let deadline = Duration::from_secs(2);
        let start = std::time::Instant::now();
        while !repository.contains(1).await && start.elapsed() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        assert!(repository.contains(1).await);
        assert!(!repository.contains(2).await);
        assert_eq!(repository.count().await, 1);

        task.await.unwrap().unwrap();
    }
}
// --8<-- [end:unit-test]
