//! A place in a stream for a consumer group: the oldest retained entry, the tail, or the entry
//! after an id. Each scenario is a test.
//!
//! ```text
//! cargo test --example redis_positions_position --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream_fred::stream::prelude::*;
use serde::{Deserialize, Serialize};

/// An order; the producer sends id 0 to mark the orders queued behind it as void.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

// --8<-- [start:position]
/// On the void marker the group moves to the tail: the orders queued behind the marker are never
/// processed, by this consumer or any other of `workers`.
#[subscriber(RedisStream::new("orders").group("workers"))]
async fn work(order: &Order, Ctx(seeker): Ctx<keys::SeekHandle>) -> HandlerOutcome {
    if order.id != 0 {
        tracing::info!(order.id, "order processed");
        return HandlerOutcome::ack();
    }
    match seeker.seek(RedisGroupPosition::end()).await {
        Ok(()) => HandlerOutcome::ack(),
        Err(_) => HandlerOutcome::retry(),
    }
}
// --8<-- [end:position]

fn app() -> impl App {
    let broker = RedisBroker::standalone("redis://localhost:6379");
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(work);
    })
}

// The scenarios are the tests below; the command in the header runs them.
fn main() {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use futures::future::join_all;
    use ruststream::testing::TestApp;

    use super::*;

    #[tokio::test]
    async fn the_orders_behind_the_marker_are_skipped() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<RedisBroker>();

        // The marker and the two orders behind it wait in the stream together.
        let queued: Vec<Order> = [1, 0, 2, 3].into_iter().map(|id| Order { id }).collect();
        for published in join_all(queued.iter().map(|order| broker.message(order).publish())).await
        {
            published?;
        }
        broker.message(&Order { id: 4 }).publish().await?;

        let worked: Vec<u64> = broker
            .subscriber("orders")
            .received::<Order>()
            .iter()
            .map(|order| order.id)
            .collect();
        assert_eq!(worked, [1, 0, 4]);
        tb.shutdown().await?;
        Ok(())
    }
}
