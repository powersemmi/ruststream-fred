//! The service of `examples/redis_broker_shutdown.rs` without the macros: the handler is a named
//! type and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_redis_broker_shutdown --features testing
//! ```

use std::convert::Infallible;
use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream_fred::stream::prelude::*;

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
struct Shipment {
    order_id: u64,
}

// `#[outgoing(name = "shipments")]` by hand: a shipment names the stream it is added to.
impl OutgoingDestination for Shipment {
    type Form = FixedName;
    const DESTINATION: &'static str = "shipments";
}

impl MessageHeaders for Shipment {
    type Contract = NoHeaders;
}

struct Ship;

impl Handle<Order, Shipment> for Ship {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Shipment, HandlerOutcome>> {
        ready(Ok(Shipment { order_id: order.id }))
    }
}

// --8<-- [start:app]
fn app() -> impl App<State = ()> {
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .with_broker(RedisBroker::standalone("redis://localhost:6379"), |b| {
            b.include(
                subscriber(RedisStream::new("orders").group("workers"), Ship)
                    .reply()
                    .build(),
            );
        })
        // Runs once every connection of the broker is closed.
        .after_shutdown(async move |_state| {
            tracing::info!("redis connections closed");
            Ok::<_, Infallible>(())
        })
}
// --8<-- [end:app]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_fred::stream::prelude::*;

    use super::{Order, Shipment, app};

    #[tokio::test]
    async fn the_service_ships_then_shuts_down() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<RedisBroker>()
            .message(&Order { id: 1 })
            .publish()
            .await?;

        tb.broker::<RedisBroker>()
            .published::<Shipment>("shipments")
            .assert_called_once()
            .with(&Shipment { order_id: 1 });
        tb.shutdown().await?;
        Ok(())
    }
}
