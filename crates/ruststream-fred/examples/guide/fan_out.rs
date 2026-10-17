//! The service of the Fan-out with Pub/Sub guide: every instance refreshes its price cache from a
//! channel, follows order events through a pattern, and announces shipped orders to every listener.
//!
//! ```text
//! docker run -p 6379:6379 redis:8.4
//! cargo run --example guide_fan_out --features testing -- run
//! ```

use ruststream_fred::prelude::*;
use serde::{Deserialize, Serialize};

// --8<-- [start:broadcast]
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "prices.changed")]
struct PriceChanged {
    sku: String,
    price_cents: u64,
}

#[subscriber(RedisPubSub::new("prices.changed").buffer(nonzero!(1_000)))]
async fn refresh_price(change: &PriceChanged) -> HandlerOutcome {
    tracing::info!(change.sku, change.price_cents, "price cache refreshed");
    HandlerOutcome::ack()
}

#[subscriber(RedisPubSub::new("prices.changed"))]
async fn reindex_price(change: &PriceChanged) -> HandlerOutcome {
    tracing::info!(change.sku, "search index updated");
    HandlerOutcome::ack()
}
// --8<-- [end:broadcast]

// --8<-- [start:pattern]
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct OrderEvent {
    order_id: u64,
}

#[subscriber(RedisPubSubPattern::new("orders.events.*"))]
async fn track_order(event: &OrderEvent, Ctx(channel): Ctx<keys::Channel>) -> HandlerOutcome {
    tracing::info!(event.order_id, channel, "order event");
    HandlerOutcome::ack()
}
// --8<-- [end:pattern]

// --8<-- [start:announce]
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "shipments")]
struct Shipment {
    order_id: u64,
}

#[subscriber(
    RedisStream::new("shipments").group("notifier"),
    reply("orders.events.shipped")
)]
async fn announce_shipment(shipment: &Shipment) -> OrderEvent {
    OrderEvent {
        order_id: shipment.order_id,
    }
}
// --8<-- [end:announce]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("storefront", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            b.include(refresh_price);
            b.include(reindex_price);
            // A glob is no channel a publish can reach, so the registration names one for copies.
            b.include(track_order)
                .out_retry(pubsub::Publish::default())
                .to("orders.events.retry");
            b.include(announce_shipment)
                .out_reply(pubsub::Publish::default());
        },
    )
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_fred::prelude::*;

    use super::{OrderEvent, PriceChanged, Shipment, app};

    #[tokio::test]
    async fn a_price_change_reaches_every_subscriber() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let change = PriceChanged {
            sku: "mug".to_owned(),
            price_cents: 900,
        };

        tb.broker::<RedisBroker>()
            .message(&change)
            .publish()
            .await?;

        tb.broker::<RedisBroker>()
            .subscriber("prices.changed")
            .assert_called(2)
            .with(&change);
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_shipment_is_announced_to_the_pattern() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<RedisBroker>()
            .message(&Shipment { order_id: 7 })
            .publish()
            .await?;

        tb.broker::<RedisBroker>()
            .published::<OrderEvent>("orders.events.shipped")
            .assert_called_once()
            .with(&OrderEvent { order_id: 7 });
        tb.broker::<RedisBroker>()
            .subscriber("orders.events.*")
            .assert_called_once()
            .with(&OrderEvent { order_id: 7 });
        tb.shutdown().await?;
        Ok(())
    }
}
