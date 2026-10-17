//! The prelude of a file that mixes forms: a stream subscription replies onto a Pub/Sub channel,
//! and a channel subscription hears the reply.
//!
//! ```text
//! cargo test --example redis_broker_prelude --features testing
//! ```

use serde::{Deserialize, Serialize};

// --8<-- [start:app]
use ruststream_fred::prelude::*;

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "shipments")]
struct Shipped {
    order_id: u64,
}

#[subscriber(RedisStream::new("orders").group("workers"), reply)]
async fn ship(order: &Order) -> Shipped {
    Shipped { order_id: order.id }
}

#[subscriber(RedisPubSub::new("shipments"))]
async fn notify(shipped: &Shipped) -> HandlerOutcome {
    tracing::info!(shipped.order_id, "customer notified");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App<State = ()> {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            // Each form's policy under its module: `stream::Publish`, `list::Publish`, `pubsub::Publish`.
            b.include(ship).out_reply(pubsub::Publish::new());
            b.include(notify);
        },
    )
}
// --8<-- [end:app]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_fred::prelude::*;

    use super::{Order, Shipped, app};

    #[tokio::test]
    async fn a_shipment_reaches_the_channel() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<RedisBroker>()
            .message(&Order { id: 3 })
            .publish()
            .await?;

        tb.broker::<RedisBroker>()
            .subscriber("shipments")
            .assert_called_once()
            .with(&Shipped { order_id: 3 })
            .settled(HandlerOutcome::ack());
        Ok(())
    }
}
