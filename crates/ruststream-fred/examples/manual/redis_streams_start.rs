//! The service of `examples/redis_streams_start.rs` written without the macros: the start of a
//! new group is a setting of the descriptor `subscriber(..)` takes.
//!
//! ```text
//! cargo test --example manual_redis_streams_start --features testing
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

// --8<-- [start:start]
/// Reports every order it reads.
struct Report;

impl Handle<Order> for Report {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, "order reported");
        ready(Ok(()))
    }
}

fn app() -> impl App {
    let broker = RedisBroker::standalone("redis://localhost:6379");
    RustStream::new(AppInfo::new("reports", "0.1.0")).with_broker(broker, |b| {
        // The first time the group is created, it reads every order the stream still holds.
        let orders = RedisStream::new("orders")
            .group("reports")
            .start_id(StreamStart::Beginning);
        b.include(subscriber(orders, Report).build());
    })
}
// --8<-- [end:start]

// The scenarios are the tests below; the command in the header runs them.
fn main() {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;

    use super::*;

    /// The group the subscription creates reads the stream from its start, and every order added
    /// afterwards.
    #[tokio::test]
    async fn the_new_group_reads_the_stream() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<RedisBroker>();

        broker.message(&Order { id: 1 }).publish().await?;
        broker.message(&Order { id: 2 }).publish().await?;

        let reported: Vec<u64> = broker
            .subscriber("orders")
            .received::<Order>()
            .iter()
            .map(|order| order.id)
            .collect();
        assert_eq!(reported, [1, 2]);
        tb.shutdown().await?;
        Ok(())
    }
}
