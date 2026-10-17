//! The service of the Choosing a transport guide: one shop service on all three Redis transports.
//! Orders are billed off a stream, thumbnails are rendered off a list, and price changes arrive
//! on a Pub/Sub channel.
//!
//! ```text
//! docker run -p 6379:6379 redis:8.4
//! cargo run --example guide_choosing_a_transport --features testing -- run
//! ```

use ruststream_fred::prelude::*;
use serde::{Deserialize, Serialize};

// --8<-- [start:stream]
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
    amount_cents: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "invoices")]
struct Invoice {
    order_id: u64,
    amount_cents: u64,
}

#[subscriber(RedisStream::new("orders").group("billing"), reply)]
async fn bill(order: &Order) -> Invoice {
    tracing::info!(order.id, "order billed");
    Invoice {
        order_id: order.id,
        amount_cents: order.amount_cents,
    }
}
// --8<-- [end:stream]

// --8<-- [start:list]
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "thumbnails")]
struct Thumbnail {
    image: String,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "thumbnails.ready")]
struct ThumbnailReady {
    image: String,
}

#[subscriber(RedisList::new("thumbnails").reliable(), reply)]
async fn render(job: &Thumbnail) -> ThumbnailReady {
    tracing::info!(job.image, "thumbnail rendered");
    ThumbnailReady {
        image: job.image.clone(),
    }
}
// --8<-- [end:list]

// --8<-- [start:pubsub]
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "prices")]
struct PriceChange {
    sku: String,
    price_cents: u64,
}

#[subscriber(RedisPubSub::new("prices"))]
async fn refresh_price(change: &PriceChange) -> HandlerOutcome {
    tracing::info!(change.sku, change.price_cents, "price cache refreshed");
    HandlerOutcome::ack()
}
// --8<-- [end:pubsub]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("shop", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            b.include(bill).out_reply(stream::Publish);
            b.include(render).out_reply(pubsub::Publish::default());
            b.include(refresh_price);
        },
    )
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
