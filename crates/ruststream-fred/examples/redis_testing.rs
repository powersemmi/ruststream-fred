//! Testing a Redis service in process: the production app, `RedisBroker` included, run by
//! `TestApp` with no Redis server. A publish through the harness returns once every handler it
//! woke has settled, so the assertions read finished state.
//!
//! ```text
//! cargo run --example redis_testing --features testing -- run
//! cargo test --example redis_testing --features testing
//! ```

use std::time::Duration;

use ruststream::runtime::RETRY_COUNT_HEADER;
use serde::{Deserialize, Serialize};

use ruststream_fred::prelude::*;

/// How long a declined invoice waits before it is tried again.
const RETRY_DELAY: Duration = Duration::from_secs(5);

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "payments")]
struct Payment {
    id: u64,
    amount: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "settlements")]
struct Settlement {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "receipts")]
struct Receipt {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "invoices")]
struct Invoice {
    id: u64,
}

#[subscriber(RedisStream::new("payments").group("workers"))]
async fn accept(payment: &Payment) -> HandlerOutcome {
    if payment.amount == 0 {
        return HandlerOutcome::drop();
    }
    HandlerOutcome::ack()
}

#[subscriber(RedisList::new("settlements").reliable(), reply)]
async fn settle(settlement: &Settlement) -> Receipt {
    Receipt { id: settlement.id }
}

/// The first attempt is declined; the copy the delay queue adds back carries the retry count.
#[subscriber(
    RedisStream::new("invoices")
        .group("workers")
        .delayed_retry(DelayedRetry::DurableZset { key: "invoices.delayed".to_owned(), ttl: None })
)]
async fn bill(invoice: &Invoice, ctx: &mut Context<'_>) -> HandlerOutcome {
    tracing::info!(invoice.id, "invoice tried");
    if ctx.headers().get_str(RETRY_COUNT_HEADER).is_some() {
        HandlerOutcome::ack()
    } else {
        HandlerOutcome::retry_after(RETRY_DELAY)
    }
}

/// The app `main` runs in production, and the one a test hands the harness unchanged.
#[ruststream::app]
fn app() -> impl App<State = ()> {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            b.include(accept);
            b.include(settle).out_reply(stream::Publish);
            b.include(bill);
        },
    )
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_fred::prelude::*;

    use super::{Invoice, Payment, RETRY_DELAY, Receipt, Settlement, app};

    // --8<-- [start:testapp]
    #[tokio::test]
    async fn a_paid_payment_is_accepted() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let payment = Payment { id: 1, amount: 100 };
        tb.broker::<RedisBroker>()
            .message(&payment)
            .publish()
            .await?;

        tb.broker::<RedisBroker>()
            .subscriber("payments")
            .assert_called_once()
            .with(&payment)
            .settled(HandlerOutcome::ack());
        Ok(())
    }
    // --8<-- [end:testapp]

    // --8<-- [start:published]
    #[tokio::test]
    async fn a_settlement_is_answered_with_a_receipt() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<RedisBroker>()
            .message(&Settlement { id: 3 })
            .publish()
            .await?;

        tb.broker::<RedisBroker>()
            .published::<Receipt>("receipts")
            .assert_called_once()
            .with(&Receipt { id: 3 });
        Ok(())
    }
    // --8<-- [end:published]

    // --8<-- [start:clock]
    #[tokio::test(start_paused = true)]
    async fn a_declined_invoice_comes_back_after_the_delay() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<RedisBroker>()
            .message(&Invoice { id: 4 })
            .publish()
            .await?;
        tb.broker::<RedisBroker>()
            .subscriber("invoices")
            .assert_called_once()
            .settled(HandlerOutcome::retry_after(RETRY_DELAY));

        // The delay queue reads the test's clock, so the retry is due once the clock moves.
        tb.advance(RETRY_DELAY).await?;
        tb.broker::<RedisBroker>()
            .subscriber("invoices")
            .assert_called(2)
            .settled(HandlerOutcome::ack());
        Ok(())
    }
    // --8<-- [end:clock]
}
