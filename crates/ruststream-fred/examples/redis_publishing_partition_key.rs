//! `partition_key` keys one published message, and `workers(n, by_key)` on the consuming side
//! runs messages of one key on one lane, in the order they were written.
//!
//! ```text
//! cargo test --example redis_publishing_partition_key --features testing
//! cargo run --example redis_publishing_partition_key -- run
//! ```

use ruststream_fred::stream::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders.in")]
struct Order {
    id: u64,
    customer: String,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders.keyed")]
struct KeyedOrder {
    id: u64,
    customer: String,
}

#[derive(OutSlot)]
#[publishes(KeyedOrder)]
struct Ledger;

// --8<-- [start:partition_key]
/// Forwards each order keyed by its customer.
#[subscriber(RedisStream::new("orders.in").group("intake"))]
async fn forward(
    order: &Order,
    Out(ledger): Out<impl Publisher<Options = RedisPublishOptions>, Ledger>,
) -> HandlerOutcome {
    let keyed = KeyedOrder {
        id: order.id,
        customer: order.customer.clone(),
    };
    let published = ledger
        .message(&keyed)
        .partition_key(order.customer.as_str())
        .publish()
        .await;
    if published.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

/// Four lanes; the orders of one customer share a lane and keep their order.
#[subscriber(RedisStream::new("orders.keyed").group("ledger"), workers(4, by_key))]
async fn book(order: &KeyedOrder) -> HandlerOutcome {
    tracing::info!(order.id, order.customer, "order booked");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("ledger", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            b.include(forward).out(Ledger, Publish).build();
            b.include(book);
        },
    )
}
// --8<-- [end:partition_key]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_fred::stream::prelude::*;

    use super::{KeyedOrder, Order, app};

    fn order(id: u64, customer: &str) -> Order {
        Order {
            id,
            customer: customer.to_owned(),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_customers_orders_keep_their_order() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        for (id, customer) in [(1, "acme"), (2, "globex"), (3, "acme"), (4, "acme")] {
            tb.broker::<RedisBroker>()
                .message(&order(id, customer))
                .publish()
                .await?;
        }
        tb.settle().await?;

        tb.broker::<RedisBroker>()
            .published::<KeyedOrder>("orders.keyed")
            .assert_called(4)
            .with_header(PARTITION_KEY_HEADER, "acme");
        let booked = tb
            .broker::<RedisBroker>()
            .subscriber("orders.keyed")
            .assert_called(4)
            .received::<KeyedOrder>();
        let acme: Vec<u64> = booked
            .iter()
            .filter(|order| order.customer == "acme")
            .map(|order| order.id)
            .collect();
        assert_eq!(acme, [1, 3, 4]);
        tb.shutdown().await?;
        Ok(())
    }
}
