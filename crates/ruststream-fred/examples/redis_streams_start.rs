//! Where a new consumer group starts: `start_id(StreamStart::Beginning)` makes a group created by
//! the subscription read what the stream already holds. Each scenario is a test.
//!
//! ```text
//! cargo test --example redis_streams_start --features testing
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

// --8<-- [start:start]
/// A reporting group added to a running system: the first time it is created, it reads every
/// order the stream still holds, then the new ones.
#[subscriber(RedisStream::new("orders").group("reports").start_id(StreamStart::Beginning))]
async fn report(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order reported");
    HandlerOutcome::ack()
}
// --8<-- [end:start]

fn app() -> impl App {
    let broker = RedisBroker::standalone("redis://localhost:6379");
    RustStream::new(AppInfo::new("reports", "0.1.0")).with_broker(broker, |b| {
        b.include(report);
    })
}

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
