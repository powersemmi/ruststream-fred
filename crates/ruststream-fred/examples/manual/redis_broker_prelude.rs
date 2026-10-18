//! The service of `examples/redis_broker_prelude.rs` without the macros: the handlers are named
//! types and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_redis_broker_prelude --features testing
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// --8<-- [start:app]
use ruststream_fred::prelude::*;

struct Ship;

impl Handle<Order, Shipped> for Ship {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Shipped, HandlerOutcome>> {
        ready(Ok(Shipped { order_id: order.id }))
    }
}

struct Notify;

impl Handle<Shipped> for Notify {
    fn handle(
        &self,
        shipped: &Shipped,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(shipped.order_id, "customer notified");
        ready(Ok(()))
    }
}

fn app() -> impl App<State = ()> {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            // Each form's policy under its module: `stream::Publish`, `list::Publish`, `pubsub::Publish`.
            b.include(
                subscriber(RedisStream::new("orders").group("workers"), Ship)
                    .reply()
                    .build(),
            )
            .out_reply(pubsub::Publish::new());
            b.include(subscriber(RedisPubSub::new("shipments"), Notify).build());
        },
    )
}
// --8<-- [end:app]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
}

// `#[outgoing(name = "orders")]` by hand: an order names the stream it is added to.
impl OutgoingDestination for Order {
    type Form = FixedName;
    const DESTINATION: &'static str = "orders";
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Shipped {
    order_id: u64,
}

// `#[outgoing(name = "shipments")]` by hand: a shipment names the channel it is published to.
impl OutgoingDestination for Shipped {
    type Form = FixedName;
    const DESTINATION: &'static str = "shipments";
}

impl MessageHeaders for Shipped {
    type Contract = NoHeaders;
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
