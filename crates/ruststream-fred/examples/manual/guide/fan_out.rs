//! The service of the Fan-out with Pub/Sub guide, written without the macros: the handlers are
//! types with an `impl Handle`, mounted with the `subscriber` constructor, and `main` is written by
//! hand.
//!
//! ```text
//! docker run -p 6379:6379 redis:8.4
//! cargo run --example manual_guide_fan_out --features testing -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_fred::prelude::*;
use serde::{Deserialize, Serialize};

/// Declares what `#[derive(Outgoing)]` declares: the destination form and no header contract.
macro_rules! outgoing {
    ($ty:ty) => {
        impl OutgoingDestination for $ty {
            type Form = CallerName;
        }

        impl MessageHeaders for $ty {
            type Contract = NoHeaders;
        }
    };
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

// --8<-- [start:broadcast]
#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct PriceChanged {
    sku: String,
    price_cents: u64,
}

outgoing!(PriceChanged, "prices.changed");

struct RefreshPrice;

impl Handle<PriceChanged> for RefreshPrice {
    fn handle(
        &self,
        change: &PriceChanged,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(change.sku, change.price_cents, "price cache refreshed");
        ready(Ok(()))
    }
}

struct ReindexPrice;

impl Handle<PriceChanged> for ReindexPrice {
    fn handle(
        &self,
        change: &PriceChanged,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(change.sku, "search index updated");
        ready(Ok(()))
    }
}
// --8<-- [end:broadcast]

// --8<-- [start:pattern]
#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct OrderEvent {
    order_id: u64,
}

outgoing!(OrderEvent);

struct TrackOrder;

impl Handle<OrderEvent, (), (), PubSubContext> for TrackOrder {
    fn handle(
        &self,
        event: &OrderEvent,
        _outs: &(),
        ctx: &mut Context<'_, PubSubContext>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        let channel = ctx.context(keys::Channel);
        tracing::info!(event.order_id, channel, "order event");
        ready(Ok(()))
    }
}
// --8<-- [end:pattern]

// --8<-- [start:announce]
#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Shipment {
    order_id: u64,
}

outgoing!(Shipment, "shipments");

struct AnnounceShipment;

impl Handle<Shipment, OrderEvent> for AnnounceShipment {
    fn handle(
        &self,
        shipment: &Shipment,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<OrderEvent, HandlerOutcome>> {
        ready(Ok(OrderEvent {
            order_id: shipment.order_id,
        }))
    }
}
// --8<-- [end:announce]

// --8<-- [start:app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("storefront", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            b.include(
                subscriber(
                    RedisPubSub::new("prices.changed").buffer(nonzero!(1_000)),
                    RefreshPrice,
                )
                .build(),
            );
            b.include(subscriber(RedisPubSub::new("prices.changed"), ReindexPrice).build());
            // A glob is no channel a publish can reach, so the registration names one for copies.
            b.include(subscriber(RedisPubSubPattern::new("orders.events.*"), TrackOrder).build())
                .out_retry(pubsub::Publish::default())
                .to("orders.events.retry");
            b.include(
                subscriber(
                    RedisStream::new("shipments").group("notifier"),
                    AnnounceShipment,
                )
                .reply()
                .to("orders.events.shipped")
                .build(),
            )
            .out_reply(pubsub::Publish::default());
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
