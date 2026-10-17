//! The service of `examples/redis_pubsub_sharded.rs` written without the macros: the handlers
//! implement `Handle`, and the reply is declared on the mount chain with `.reply()`.
//!
//! ```text
//! cargo test --example manual_redis_pubsub_sharded --features testing
//! cargo run --example manual_redis_pubsub_sharded --features testing -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::{self, JsonSchema};
use ruststream_fred::pubsub::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[outgoing(name = "orders.eu")]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[outgoing(name = "shipments.eu")]
struct Shipment {
    order: u64,
}

/// The shipment feed, read sharded as well.
struct Track;

impl Handle<Shipment> for Track {
    fn handle(
        &self,
        shipment: &Shipment,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(shipment.order, "shipment tracked");
        ready(Ok(()))
    }
}

// --8<-- [start:sharded]
/// Reads `orders.eu` with `SSUBSCRIBE` and answers on `shipments.eu`.
struct Ship;

impl Handle<Order, Shipment> for Ship {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Shipment, HandlerOutcome>> {
        tracing::info!(order.id, "shipping");
        ready(Ok(Shipment { order: order.id }))
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("shipping", "0.1.0")).with_broker(
        RedisBroker::cluster(["redis://localhost:7000"]),
        |b| {
            let orders = RedisPubSub::new("orders.eu").mode(PubSubMode::Sharded);
            // The reply leaves with `SPUBLISH`: a sharded subscriber hears only a sharded publish.
            b.include(subscriber(orders, Ship).reply().build())
                .out_reply(Publish::new().mode(PubSubMode::Sharded));
            let shipments = RedisPubSub::new("shipments.eu").mode(PubSubMode::Sharded);
            b.include(subscriber(shipments, Track).build());
        },
    )
}
// --8<-- [end:sharded]

fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_fred::pubsub::prelude::*;

    use super::{Order, Shipment, app};

    #[tokio::test]
    async fn a_sharded_order_is_answered_on_a_sharded_channel() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<RedisBroker>()
            .message(&Order { id: 9 })
            .publish()
            .await?;

        tb.broker::<RedisBroker>()
            .subscriber("orders.eu")
            .assert_called_once()
            .with(&Order { id: 9 });
        tb.broker::<RedisBroker>()
            .published::<Shipment>("shipments.eu")
            .assert_called_once()
            .with(&Shipment { order: 9 });
        tb.broker::<RedisBroker>()
            .subscriber("shipments.eu")
            .assert_called_once()
            .with(&Shipment { order: 9 });
        Ok(())
    }
}
