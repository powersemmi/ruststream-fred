//! The service of `examples/redis_publishing_partition_key.rs` written without `#[subscriber]`:
//! the slot is declared by hand, and the keyed lanes are the `workers_by_key` step.
//!
//! ```text
//! cargo test --example manual_redis_publishing_partition_key --features testing
//! cargo run --example manual_redis_publishing_partition_key -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_fred::stream::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "orders.in")]
struct Order {
    id: u64,
    customer: String,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "orders.keyed")]
struct KeyedOrder {
    id: u64,
    customer: String,
}

struct Ledger;

impl OutSlot for Ledger {
    const NAME: &'static str = "Ledger";
    type Destination = Reads;
}

impl PublishedThrough<Ledger> for KeyedOrder {}

// --8<-- [start:partition_key]
/// Forwards each order keyed by its customer.
struct Forward;

impl<L> Handle<Order, (), Outs<(L,)>> for Forward
where
    L: OutEntry<Ledger, Wire: Publisher<Options = RedisPublishOptions>>,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(L,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let keyed = KeyedOrder {
            id: order.id,
            customer: order.customer.clone(),
        };
        outs.get(Ledger)
            .message(&keyed)
            .partition_key(order.customer.as_str())
            .publish()
            .await
            .map_err(|_| HandlerOutcome::retry())
    }
}

/// Books an order; the lanes keep one customer's orders in order.
struct Book;

impl Handle<KeyedOrder> for Book {
    fn handle(
        &self,
        order: &KeyedOrder,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, order.customer, "order booked");
        ready(Ok(()))
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("ledger", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            let forward = subscriber(RedisStream::new("orders.in").group("intake"), Forward);
            b.include(forward.build()).out(Ledger, Publish).build();
            // Four lanes; the orders of one customer share a lane and keep their order.
            let book = subscriber(RedisStream::new("orders.keyed").group("ledger"), Book);
            b.include(book.workers_by_key(nonzero!(4)).build());
        },
    )
}
// --8<-- [end:partition_key]

fn main() -> ExitCode {
    run_main(app)
}

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
