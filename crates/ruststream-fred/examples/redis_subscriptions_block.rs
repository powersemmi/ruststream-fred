//! How long one read waits on the server: `block` on the descriptor, or the `block` step after a
//! batch size at the mount site. Each scenario is a test.
//!
//! ```text
//! cargo test --example redis_subscriptions_block --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::time::Duration;

use ruststream_fred::stream::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

// --8<-- [start:block]
/// Each read of `orders` waits at most one second on the server.
#[subscriber(RedisStream::new("orders").group("workers").block(Duration::from_secs(1)))]
async fn place(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order placed");
    HandlerOutcome::ack()
}

/// Bills up to 16 orders a read.
#[subscriber(RedisStream::new("invoices").group("billing"))]
async fn bill(orders: &[Order]) -> HandlerOutcome {
    tracing::info!(count = orders.len(), "orders billed");
    HandlerOutcome::ack()
}

fn app() -> impl App {
    let broker = RedisBroker::standalone("redis://localhost:6379");
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(place);
        // The size is the framework's step and the wait is Redis's, chained after it.
        b.include(bill.batch(nonzero!(16)).block(Duration::from_secs(2)));
    })
}
// --8<-- [end:block]

// The scenarios are the tests below; the command in the header runs them.
fn main() {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;

    use super::*;

    #[tokio::test]
    async fn a_read_with_its_own_wait_delivers() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<RedisBroker>();

        broker
            .message(&Order { id: 1 })
            .to("orders")
            .publish()
            .await?;
        broker
            .message(&Order { id: 2 })
            .to("invoices")
            .publish()
            .await?;

        broker
            .subscriber("orders")
            .assert_called_once()
            .with(&Order { id: 1 })
            .settled(HandlerOutcome::ack());
        assert_eq!(
            broker.subscriber("invoices").batches::<Order>(),
            [vec![Order { id: 2 }]]
        );
        tb.shutdown().await?;
        Ok(())
    }
}
