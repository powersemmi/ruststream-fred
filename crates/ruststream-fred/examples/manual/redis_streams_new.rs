//! The service of `examples/redis_streams_new.rs` written without the macros: a `RedisStream`
//! descriptor is the source `subscriber(..)` takes.
//!
//! ```text
//! cargo test --example manual_redis_streams_new --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::future::{Future, ready};

use ruststream::schemars::{self, JsonSchema};
use ruststream_fred::stream::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
struct Order {
    id: u64,
}

// `#[outgoing(name = "orders")]` by hand: the type fixes its destination.
impl OutgoingDestination for Order {
    type Form = FixedName;

    const DESTINATION: &'static str = "orders";
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

// --8<-- [start:groups]
/// Bills every order.
struct Bill;

impl Handle<Order> for Bill {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, "order billed");
        ready(Ok(()))
    }
}

/// Ships every order.
struct Ship;

impl Handle<Order> for Ship {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, "order shipped");
        ready(Ok(()))
    }
}

fn app() -> impl App {
    let broker = RedisBroker::standalone("redis://localhost:6379");
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        let billing = RedisStream::new("orders").group("billing");
        b.include(subscriber(billing, Bill).build());
        // A second group reads the same entries again.
        let shipping = RedisStream::new("orders")
            .group("shipping")
            .consumer("shipping-1");
        b.include(subscriber(shipping, Ship).build());
    })
}
// --8<-- [end:groups]

// The scenarios are the tests below; the command in the header runs them.
fn main() {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;

    use super::*;

    #[tokio::test]
    async fn every_group_reads_every_entry() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        let broker = tb.broker::<RedisBroker>();
        broker.message(&Order { id: 1 }).publish().await?;
        broker.message(&Order { id: 2 }).publish().await?;

        // Both handlers read `orders`, so the harness lists their deliveries under that name.
        let read: Vec<u64> = broker
            .subscriber("orders")
            .received::<Order>()
            .iter()
            .map(|order| order.id)
            .collect();
        assert_eq!(read, [1, 1, 2, 2]);
        broker
            .subscriber("orders")
            .assert_called(4)
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
}
