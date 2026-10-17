//! The service of the Choosing a transport guide, written without the macros: the handlers are
//! types with an `impl Handle`, mounted with the `subscriber` constructor, and `main` is written by
//! hand.
//!
//! ```text
//! docker run -p 6379:6379 redis:8.4
//! cargo run --example manual_guide_choosing_a_transport --features testing -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_fred::prelude::*;
use serde::{Deserialize, Serialize};

/// Declares the destination a type names, as `#[outgoing(name = "..")]` does.
macro_rules! named {
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

// --8<-- [start:stream]
#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
    amount_cents: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Invoice {
    order_id: u64,
    amount_cents: u64,
}

named!(Order, "orders");
named!(Invoice, "invoices");

struct Bill;

impl Handle<Order, Invoice> for Bill {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Invoice, HandlerOutcome>> {
        tracing::info!(order.id, "order billed");
        ready(Ok(Invoice {
            order_id: order.id,
            amount_cents: order.amount_cents,
        }))
    }
}
// --8<-- [end:stream]

// --8<-- [start:list]
#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Thumbnail {
    image: String,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct ThumbnailReady {
    image: String,
}

named!(Thumbnail, "thumbnails");
named!(ThumbnailReady, "thumbnails.ready");

struct Render;

impl Handle<Thumbnail, ThumbnailReady> for Render {
    fn handle(
        &self,
        job: &Thumbnail,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<ThumbnailReady, HandlerOutcome>> {
        tracing::info!(job.image, "thumbnail rendered");
        ready(Ok(ThumbnailReady {
            image: job.image.clone(),
        }))
    }
}
// --8<-- [end:list]

// --8<-- [start:pubsub]
#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct PriceChange {
    sku: String,
    price_cents: u64,
}

named!(PriceChange, "prices");

struct RefreshPrice;

impl Handle<PriceChange> for RefreshPrice {
    fn handle(
        &self,
        change: &PriceChange,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(change.sku, change.price_cents, "price cache refreshed");
        ready(Ok(()))
    }
}
// --8<-- [end:pubsub]

// --8<-- [start:app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("shop", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            b.include(
                subscriber(RedisStream::new("orders").group("billing"), Bill)
                    .reply()
                    .build(),
            )
            .out_reply(stream::Publish);
            b.include(
                subscriber(RedisList::new("thumbnails").reliable(), Render)
                    .reply()
                    .build(),
            )
            .out_reply(pubsub::Publish::default());
            b.include(subscriber(RedisPubSub::new("prices"), RefreshPrice).build());
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

    use super::{Invoice, Order, PriceChange, Thumbnail, ThumbnailReady, app};

    #[tokio::test]
    async fn an_order_is_billed_on_the_invoices_stream() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<RedisBroker>()
            .message(&Order {
                id: 1,
                amount_cents: 1250,
            })
            .publish()
            .await?;

        tb.broker::<RedisBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.broker::<RedisBroker>()
            .published::<Invoice>("invoices")
            .assert_called_once()
            .with(&Invoice {
                order_id: 1,
                amount_cents: 1250,
            });
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_rendered_thumbnail_is_announced_on_a_channel() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<RedisBroker>()
            .message(&Thumbnail {
                image: "cat.png".to_owned(),
            })
            .publish()
            .await?;

        tb.broker::<RedisBroker>()
            .subscriber("thumbnails")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.broker::<RedisBroker>()
            .published::<ThumbnailReady>("thumbnails.ready")
            .assert_called_once()
            .with(&ThumbnailReady {
                image: "cat.png".to_owned(),
            });
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_price_change_reaches_the_cache() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let change = PriceChange {
            sku: "mug".to_owned(),
            price_cents: 900,
        };

        tb.broker::<RedisBroker>()
            .message(&change)
            .publish()
            .await?;

        tb.broker::<RedisBroker>()
            .subscriber("prices")
            .assert_called_once()
            .with(&change);
        tb.shutdown().await?;
        Ok(())
    }
}
