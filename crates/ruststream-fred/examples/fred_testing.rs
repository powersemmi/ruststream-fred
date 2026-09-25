//! Testing a Redis service in process: the harness runs the app `main` runs, with its
//! `RedisBroker` connected to an in-process Redis instead of a server.
//!
//! A service enables this crate's `testing` feature in its `[dev-dependencies]`, hands its own app
//! to `TestApp::start`, publishes through the harness and asserts on what the handlers received
//! and published. The same test body runs against a server with `TestApp::start_live`.
//!
//! This example is a test driver rather than a service, so it runs on a plain `#[tokio::main]`
//! instead of the `#[ruststream::app]` macro.
//!
//! ```text
//! cargo run --example fred_testing --features testing
//! ```

use std::sync::Arc;

use ruststream::testing::TestApp;
// This example drives all three forms, so it takes the crate prelude rather than one form's.
use ruststream_fred::prelude::*;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

#[derive(Debug, Deserialize, Outgoing, Serialize, Clone, PartialEq)]
struct Payment {
    id: u64,
    amount: u64,
}

/// The reply a settled payment publishes to the `receipts` stream.
#[derive(Debug, Deserialize, Outgoing, Serialize, PartialEq)]
#[outgoing(name = "receipts")]
struct Receipt {
    id: u64,
}

/// A repository. In production it wraps a database client; the app state is whatever the
/// startup hook builds, so a test reads back what the handler stored.
#[derive(Clone, Default)]
struct PaymentRepository {
    payments: Arc<Mutex<Vec<Payment>>>,
}

impl PaymentRepository {
    async fn save(&self, payment: Payment) {
        self.payments.lock().await.push(payment);
    }

    async fn ids(&self) -> Vec<u64> {
        self.payments.lock().await.iter().map(|p| p.id).collect()
    }
}

/// Validates a payment, stores it, and drops one with no amount.
#[subscriber(RedisStream::new("payments").group("workers"))]
async fn process_payment(
    payment: &Payment,
    ctx: &mut Context<'_, (), PaymentRepository>,
) -> HandlerOutcome {
    if payment.amount == 0 {
        return HandlerOutcome::drop();
    }
    ctx.state().save(payment.clone()).await;
    HandlerOutcome::ack()
}

/// Settles a payment taken off a work queue and answers with a receipt.
#[subscriber(RedisList::new("settlements").reliable(), publish)]
async fn settle_payment(payment: &Payment) -> Receipt {
    Receipt { id: payment.id }
}

/// Logs every payment notification.
#[subscriber(RedisPubSub::new("notifications"))]
async fn notify(payment: &Payment) -> HandlerOutcome {
    println!("notified of payment {}", payment.id);
    HandlerOutcome::ack()
}

/// The service's app: the one `main` runs, and the one its tests hand the harness.
fn app(repository: PaymentRepository) -> impl App {
    RustStream::new(AppInfo::new("payments", "0.1.0"))
        .on_startup(move |()| async move { Ok::<_, std::convert::Infallible>(repository) })
        .with_broker(RedisBroker::standalone("redis://localhost:6379"), |b| {
            b.include(process_payment);
            b.include(settle_payment).out_reply(stream::Publish);
            b.include(notify);
        })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let repository = PaymentRepository::default();
    // The harness connects the app's broker in process and drives every publish to quiescence,
    // so the assertions below need no waiting.
    let tb = TestApp::start(app(repository.clone())).await?;

    tb.broker::<RedisBroker>()
        .message(&Payment { id: 1, amount: 100 })
        .to("payments")
        .publish()
        .await?;
    tb.broker::<RedisBroker>()
        .message(&Payment { id: 2, amount: 0 })
        .to("payments")
        .publish()
        .await?;
    tb.broker::<RedisBroker>()
        .subscriber("payments")
        .assert_called(2)
        .settled(HandlerOutcome::drop());
    assert_eq!(repository.ids().await, [1]);

    tb.broker::<RedisBroker>()
        .message(&Payment { id: 3, amount: 50 })
        .to("settlements")
        .publish()
        .await?;
    tb.broker::<RedisBroker>()
        .published::<Receipt>("receipts")
        .assert_called_once()
        .with(&Receipt { id: 3 });

    tb.broker::<RedisBroker>()
        .message(&Payment { id: 4, amount: 10 })
        .to("notifications")
        .publish()
        .await?;
    tb.broker::<RedisBroker>()
        .subscriber("notifications")
        .assert_called_once();

    tb.shutdown().await?;
    Ok(())
}
