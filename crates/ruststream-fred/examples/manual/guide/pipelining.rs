//! The service of the Side effects with the acknowledgement guide, written without the macros:
//! the handlers are types with an `impl Handle`, mounted with the `subscriber` constructor, and
//! `main` is written by hand.
//!
//! ```text
//! docker run -p 6379:6379 redis:8.4
//! cargo run --example manual_guide_pipelining --features testing -- run
//! ```

use std::process::ExitCode;

use fred::interfaces::KeysInterface;
use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_fred::stream::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
    lines: Vec<Line>,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Line {
    sku: String,
    quantity: u32,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "receipts")]
struct Receipt {
    order_id: u64,
    number: i64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Payment {
    id: u64,
    account: String,
    amount_cents: i64,
}

// --8<-- [start:accept]
/// Puts every line of an order on the warehouse's pick queue and answers with a numbered receipt.
struct Accept;

impl Handle<Order, Receipt, (), PipelineContext> for Accept {
    async fn handle(
        &self,
        order: &Order,
        _outs: &(),
        ctx: &mut Context<'_, PipelineContext>,
    ) -> Result<Receipt, HandlerOutcome> {
        let pipeline = ctx.context(keys::Pipeline);
        for line in &order.lines {
            if line.quantity == 0 {
                tracing::warn!(order.id, line.sku, "an empty line drops the order");
                return Err(HandlerOutcome::drop());
            }
            if pipeline.lpush("picks", line.sku.as_str()).await.is_err() {
                return Err(HandlerOutcome::retry());
            }
        }
        // The receipt needs its number now, so the `INCR` goes over the pool, not into the round.
        let number = ctx
            .context(keys::FredPool)
            .incr::<i64, _>("receipts:sequence")
            .await
            .map_err(|err| {
                tracing::warn!(order.id, %err, "the receipt sequence is out of reach");
                HandlerOutcome::retry()
            })?;
        Ok(Receipt {
            order_id: order.id,
            number,
        })
    }
}
// --8<-- [end:accept]

// --8<-- [start:capture]
/// Books a payment: the ledger entry, the balance and the acknowledgement commit as one block.
struct Capture;

impl Handle<Payment, (), (), PipelineContext> for Capture {
    async fn handle(
        &self,
        payment: &Payment,
        _outs: &(),
        ctx: &mut Context<'_, PipelineContext>,
    ) -> Result<(), HandlerOutcome> {
        let pipeline = ctx.context(keys::Pipeline);
        let entry = format!("{}:{}", payment.id, payment.amount_cents);
        if pipeline.lpush("{payments}:ledger", entry).await.is_err()
            || pipeline
                .hincrby(
                    "{payments}:balances",
                    payment.account.as_str(),
                    payment.amount_cents,
                )
                .await
                .is_err()
        {
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}
// --8<-- [end:capture]

// --8<-- [start:app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            let accept = subscriber(RedisStream::new("orders").group("fulfilment"), Accept);
            // The receipt joins the delivery's round and leaves after the picks.
            b.include(accept.pipeline().reply().build())
                .out_reply(Publish)
                .transform(InRound);
            let capture = subscriber(RedisStream::new("{payments}").group("ledger"), Capture);
            b.include(capture.pipeline().atomic().build());
        },
    )
}

fn main() -> ExitCode {
    run_main(app)
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_fred::stream::prelude::*;

    use super::{Line, Order, Payment, Receipt, app};

    fn line(sku: &str, quantity: u32) -> Line {
        Line {
            sku: sku.to_owned(),
            quantity,
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_accepted_order_sends_its_picks_and_its_receipt() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let order = Order {
            id: 7,
            lines: vec![line("apple", 2), line("pear", 1)],
        };

        tb.broker::<RedisBroker>().message(&order).publish().await?;

        tb.broker::<RedisBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        let picks: Vec<Vec<u8>> = tb
            .broker::<RedisBroker>()
            .published::<()>("picks")
            .assert_called(2)
            .messages()
            .iter()
            .map(|pick| pick.payload().to_vec())
            .collect();
        assert_eq!(picks, [b"apple".to_vec(), b"pear".to_vec()]);
        tb.broker::<RedisBroker>()
            .published::<Receipt>("receipts")
            .assert_called_once()
            .with(&Receipt {
                order_id: 7,
                number: 1,
            });
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_dropped_order_sends_none_of_its_queued_picks() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let order = Order {
            id: 8,
            lines: vec![line("apple", 2), line("pear", 0)],
        };

        tb.broker::<RedisBroker>().message(&order).publish().await?;

        tb.broker::<RedisBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.broker::<RedisBroker>()
            .published::<()>("picks")
            .assert_not_called();
        tb.broker::<RedisBroker>()
            .published::<Receipt>("receipts")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_captured_payment_is_entered_in_the_ledger() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let payment = Payment {
            id: 3,
            account: "acme".to_owned(),
            amount_cents: 1250,
        };

        tb.broker::<RedisBroker>()
            .message(&payment)
            .to("{payments}")
            .publish()
            .await?;

        tb.broker::<RedisBroker>()
            .subscriber("{payments}")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.broker::<RedisBroker>()
            .published::<()>("{payments}:ledger")
            .assert_called_once()
            .with_raw(b"3:1250");
        tb.shutdown().await?;
        Ok(())
    }
}
