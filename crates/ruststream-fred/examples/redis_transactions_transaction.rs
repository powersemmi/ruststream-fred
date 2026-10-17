//! The owned kind: `transaction()` on a slot hands each delivery a transaction that owns its
//! buffer, so the workers of one subscription never share one.
//!
//! ```text
//! cargo test --example redis_transactions_transaction --features testing
//! cargo run --example redis_transactions_transaction -- run
//! ```

use ruststream_fred::stream::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
    items: Vec<String>,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "shipments")]
struct Shipment {
    order_id: u64,
    item: String,
}

#[derive(OutSlot)]
#[publishes(Shipment)]
struct Shipments;

// --8<-- [start:transaction]
/// One shipment per item, committed as one block; four orders ship at once.
#[subscriber(RedisStream::new("orders").group("shipping"), workers(4))]
async fn ship(
    order: &Order,
    Out(shipments): Out<impl OwnedTransactions, Shipments>,
) -> HandlerOutcome {
    let Ok(mut txn) = shipments.transaction().await else {
        return HandlerOutcome::retry();
    };
    for item in &order.items {
        if item.is_empty() {
            tracing::warn!(order.id, "an item has no name, the order ships nothing");
            // The abort discards the shipments the transaction buffered so far.
            return match txn.abort().await {
                Ok(()) => HandlerOutcome::drop(),
                Err(_) => HandlerOutcome::retry(),
            };
        }
        let shipment = Shipment {
            order_id: order.id,
            item: item.clone(),
        };
        if txn.message(&shipment).publish().await.is_err() {
            return HandlerOutcome::retry();
        }
    }
    match txn.commit().await {
        Ok(()) => HandlerOutcome::ack(),
        Err(_) => HandlerOutcome::retry(),
    }
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("shipping", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            b.include(ship).out(Shipments, TransactionalPublish).build();
        },
    )
}
// --8<-- [end:transaction]

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
