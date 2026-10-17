//! Sharded Redis Pub/Sub on a cluster: `SSUBSCRIBE` and `SPUBLISH` keep a channel's traffic on the
//! node that owns its slot.
//!
//! ```text
//! cargo test --example redis_pubsub_sharded --features testing
//! cargo run --example redis_pubsub_sharded --features testing -- run
//! ```

use ruststream_fred::pubsub::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders.eu")]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "shipments.eu")]
struct Shipment {
    order: u64,
}

/// The shipment feed, read sharded as well.
#[subscriber(RedisPubSub::new("shipments.eu").mode(PubSubMode::Sharded))]
async fn track(shipment: &Shipment) -> HandlerOutcome {
    tracing::info!(shipment.order, "shipment tracked");
    HandlerOutcome::ack()
}

// --8<-- [start:sharded]
/// Reads `orders.eu` with `SSUBSCRIBE` and answers on `shipments.eu`.
#[subscriber(RedisPubSub::new("orders.eu").mode(PubSubMode::Sharded), reply)]
async fn ship(order: &Order) -> Shipment {
    tracing::info!(order.id, "shipping");
    Shipment { order: order.id }
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("shipping", "0.1.0")).with_broker(
        RedisBroker::cluster(["redis://localhost:7000"]),
        |b| {
            // The reply leaves with `SPUBLISH`: a sharded subscriber hears only a sharded publish.
            b.include(ship)
                .out_reply(Publish::new().mode(PubSubMode::Sharded));
            b.include(track);
        },
    )
}
// --8<-- [end:sharded]

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
