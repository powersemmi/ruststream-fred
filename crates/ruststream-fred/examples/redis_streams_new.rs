//! A stream read through consumer groups: `RedisStream::new` reads fresh entries off the tail,
//! and every group reads every entry. Each scenario is a test.
//!
//! ```text
//! cargo test --example redis_streams_new --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream_fred::stream::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

// --8<-- [start:groups]
/// Bills every order, in the group `billing`.
#[subscriber(RedisStream::new("orders").group("billing"))]
async fn bill(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order billed");
    HandlerOutcome::ack()
}

/// Ships every order, in the group `shipping`: a second group reads the same entries again.
#[subscriber(RedisStream::new("orders").group("shipping").consumer("shipping-1"))]
async fn ship(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order shipped");
    HandlerOutcome::ack()
}

fn app() -> impl App {
    let broker = RedisBroker::standalone("redis://localhost:6379");
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(bill);
        b.include(ship);
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
