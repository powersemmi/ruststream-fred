//! The service and tests of `examples/redis_testing.rs` without the macros: the handlers are named
//! types and `main` is written by hand.
//!
//! ```text
//! cargo run --example manual_redis_testing --features testing -- run
//! cargo test --example manual_redis_testing --features testing
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;
use std::time::Duration;

use ruststream::runtime::RETRY_COUNT_HEADER;
use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream_fred::prelude::*;

/// How long a declined invoice waits before it is tried again.
const RETRY_DELAY: Duration = Duration::from_secs(5);

/// Declares the destination a type names, as `#[outgoing(name = "..")]` does.
macro_rules! named {
    ($ty:ty, $name:literal) => {
        impl OutgoingDestination for $ty {
            type Form = FixedName;
            const DESTINATION: &'static str = $name;
        }

        impl MessageHeaders for $ty {
            type Contract = NoHeaders;
        }
    };
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Payment {
    id: u64,
    amount: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Settlement {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Receipt {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Invoice {
    id: u64,
}

named!(Payment, "payments");
named!(Settlement, "settlements");
named!(Receipt, "receipts");
named!(Invoice, "invoices");

struct Accept;

impl Handle<Payment> for Accept {
    fn handle(
        &self,
        payment: &Payment,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        if payment.amount == 0 {
            return ready(Err(HandlerOutcome::drop()));
        }
        ready(Ok(()))
    }
}

struct Settle;

impl Handle<Settlement, Receipt> for Settle {
    fn handle(
        &self,
        settlement: &Settlement,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Receipt, HandlerOutcome>> {
        ready(Ok(Receipt { id: settlement.id }))
    }
}

/// The first attempt is declined; the copy the delay queue adds back carries the retry count.
struct Bill;

impl Handle<Invoice> for Bill {
    fn handle(
        &self,
        invoice: &Invoice,
        _outs: &(),
        ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(invoice.id, "invoice tried");
        if ctx.headers().get_str(RETRY_COUNT_HEADER).is_some() {
            ready(Ok(()))
        } else {
            ready(Err(HandlerOutcome::retry_after(RETRY_DELAY)))
        }
    }
}

/// The app `main` runs in production, and the one a test hands the harness unchanged.
fn app() -> impl App<State = ()> {
    let invoices =
        RedisStream::new("invoices")
            .group("workers")
            .delayed_retry(DelayedRetry::DurableZset {
                key: "invoices.delayed".to_owned(),
                ttl: None,
            });
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            b.include(subscriber(RedisStream::new("payments").group("workers"), Accept).build());
            b.include(
                subscriber(RedisList::new("settlements").reliable(), Settle)
                    .reply()
                    .build(),
            )
            .out_reply(stream::Publish);
            b.include(subscriber(invoices, Bill).build());
        },
    )
}

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
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
