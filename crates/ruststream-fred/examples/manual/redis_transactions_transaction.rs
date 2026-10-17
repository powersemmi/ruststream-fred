//! The service of `examples/redis_transactions_transaction.rs` written without `#[subscriber]`:
//! the body opens the transaction on the slot entry it was handed.
//!
//! ```text
//! cargo test --example manual_redis_transactions_transaction --features testing
//! cargo run --example manual_redis_transactions_transaction -- run
//! ```

use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_fred::stream::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
    items: Vec<String>,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "shipments")]
struct Shipment {
    order_id: u64,
    item: String,
}

struct Shipments;

impl OutSlot for Shipments {
    const NAME: &'static str = "Shipments";
    type Destination = Reads;
}

impl PublishedThrough<Shipments> for Shipment {}

// --8<-- [start:transaction]
/// One shipment per item, committed as one block; four orders ship at once.
struct Ship;

impl<S> Handle<Order, (), Outs<(S,)>> for Ship
where
    S: OutEntry<Shipments, Wire: OwnedTransactions>,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(S,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let mut txn = outs
            .get(Shipments)
            .transaction()
            .await
            .map_err(|_| HandlerOutcome::retry())?;
        for item in &order.items {
            if item.is_empty() {
                tracing::warn!(order.id, "an item has no name, the order ships nothing");
                // The abort discards the shipments the transaction buffered so far.
                txn.abort().await.map_err(|_| HandlerOutcome::retry())?;
                return Err(HandlerOutcome::drop());
            }
            let shipment = Shipment {
                order_id: order.id,
                item: item.clone(),
            };
            txn.message(&shipment)
                .publish()
                .await
                .map_err(|_| HandlerOutcome::retry())?;
        }
        txn.commit().await.map_err(|_| HandlerOutcome::retry())
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("shipping", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            let ship = subscriber(RedisStream::new("orders").group("shipping"), Ship);
            b.include(ship.workers(nonzero!(4)).build())
                .out(Shipments, TransactionalPublish)
                .build();
        },
    )
}
// --8<-- [end:transaction]

fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_fred::stream::prelude::*;

    use super::{Order, Shipment, app};

    fn order(id: u64, items: &[&str]) -> Order {
        Order {
            id,
            items: items.iter().map(|item| (*item).to_owned()).collect(),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_order_ships_every_item() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<RedisBroker>()
            .message(&order(1, &["tea", "cake"]))
            .publish()
            .await?;

        tb.broker::<RedisBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        let items: Vec<String> = tb
            .broker::<RedisBroker>()
            .published::<Shipment>("shipments")
            .assert_called(2)
            .decoded()
            .into_iter()
            .map(|shipment| shipment.item)
            .collect();
        assert_eq!(items, ["tea", "cake"]);
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_aborted_order_ships_nothing() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<RedisBroker>()
            .message(&order(2, &["tea", ""]))
            .publish()
            .await?;

        tb.broker::<RedisBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.broker::<RedisBroker>()
            .published::<Shipment>("shipments")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }
}
