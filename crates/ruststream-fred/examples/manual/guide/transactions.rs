//! The service of the Transactions guide, written without the macros: the handlers are types with
//! an `impl Handle`, mounted with the `subscriber` constructor, and `main` is written by hand.
//!
//! ```text
//! docker run -p 6379:6379 redis:8.4
//! cargo run --example manual_guide_transactions --features testing -- run
//! ```

use std::future::{Future, ready};
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
#[outgoing(name = "confirmations")]
struct Confirmation {
    order_id: u64,
}

/// An order the warehouse has packed.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "packed")]
struct Parcel {
    order_id: u64,
    items: Vec<String>,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "shipments")]
struct Shipment {
    order_id: u64,
    item: String,
}

// What `#[derive(OutSlot)]` with `#[publishes(Shipment)]` writes.
struct Shipments;

impl OutSlot for Shipments {
    const NAME: &'static str = "Shipments";
    type Destination = Reads;
}

impl PublishedThrough<Shipments> for Shipment {}

// --8<-- [start:confirm]
/// Confirms a batch of orders. The confirmations of one batch are published together or not at
/// all.
struct Confirm;

impl Handle<[Order], Vec<Confirmation>> for Confirm {
    fn handle(
        &self,
        orders: &[Order],
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Vec<Confirmation>, Vec<HandlerOutcome>>> {
        if orders.iter().any(|order| order.items.is_empty()) {
            tracing::warn!(
                orders = orders.len(),
                "a batch with an empty order is dropped"
            );
            return ready(Err(orders.iter().map(|_| HandlerOutcome::drop()).collect()));
        }
        ready(Ok(orders
            .iter()
            .map(|order| Confirmation { order_id: order.id })
            .collect()))
    }
}
// --8<-- [end:confirm]

// --8<-- [start:ship]
/// Publishes one shipment per item of a parcel, committed as one block.
struct Ship;

impl<Slot> Handle<Parcel, (), Outs<(Slot,)>> for Ship
where
    Slot: OutEntry<Shipments, Wire: OwnedTransactions>,
{
    async fn handle(
        &self,
        parcel: &Parcel,
        outs: &Outs<(Slot,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let mut transaction = outs
            .get(Shipments)
            .transaction()
            .await
            .map_err(|_| HandlerOutcome::retry())?;
        for item in &parcel.items {
            if item.is_empty() {
                tracing::warn!(parcel.order_id, "a nameless item, the parcel ships nothing");
                // The abort discards the shipments buffered so far.
                transaction
                    .abort()
                    .await
                    .map_err(|_| HandlerOutcome::retry())?;
                return Err(HandlerOutcome::drop());
            }
            let shipment = Shipment {
                order_id: parcel.order_id,
                item: item.clone(),
            };
            transaction
                .message(&shipment)
                .publish()
                .await
                .map_err(|_| HandlerOutcome::retry())?;
        }
        transaction
            .commit()
            .await
            .map_err(|_| HandlerOutcome::retry())
    }
}
// --8<-- [end:ship]

// --8<-- [start:app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            let confirm = subscriber(RedisStream::new("orders").group("confirmations"), Confirm);
            b.include(confirm.batch(nonzero!(32)).reply().build())
                .out_reply(TransactionalPublish)
                .transactional();
            let ship = subscriber(RedisStream::new("packed").group("shipping"), Ship);
            b.include(ship.build())
                .out(Shipments, TransactionalPublish)
                .build();
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

    use futures::future::join_all;
    use ruststream::testing::TestApp;
    use ruststream_fred::stream::prelude::*;

    use super::{Confirmation, Order, Parcel, Shipment, app};

    fn items(items: &[&str]) -> Vec<String> {
        items.iter().map(|item| (*item).to_owned()).collect()
    }

    fn order(id: u64, names: &[&str]) -> Order {
        Order {
            id,
            items: items(names),
        }
    }

    fn parcel(order_id: u64, names: &[&str]) -> Parcel {
        Parcel {
            order_id,
            items: items(names),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_batch_is_confirmed_in_one_block() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<RedisBroker>();
        let orders = [order(1, &["tea"]), order(2, &["cake"])];

        for published in join_all(orders.iter().map(|order| broker.message(order).publish())).await
        {
            published?;
        }

        let confirmed: Vec<u64> = broker
            .published::<Confirmation>("confirmations")
            .assert_called(2)
            .decoded()
            .into_iter()
            .map(|confirmation| confirmation.order_id)
            .collect();
        assert_eq!(confirmed, [1, 2]);
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_dropped_batch_confirms_nothing() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<RedisBroker>()
            .message(&order(3, &[]))
            .publish()
            .await?;

        tb.broker::<RedisBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.broker::<RedisBroker>()
            .published::<Confirmation>("confirmations")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_parcel_ships_every_item_together() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<RedisBroker>()
            .message(&parcel(4, &["tea", "cake"]))
            .publish()
            .await?;

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
    async fn an_aborted_parcel_ships_nothing() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<RedisBroker>()
            .message(&parcel(5, &["tea", ""]))
            .publish()
            .await?;

        tb.broker::<RedisBroker>()
            .subscriber("packed")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.broker::<RedisBroker>()
            .published::<Shipment>("shipments")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }
}
