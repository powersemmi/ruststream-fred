//! The claiming read mode: one subscription reads the fresh tail and takes back the group's
//! entries left pending at least `min_idle`, in one `XREADGROUP ... CLAIM`. Each scenario is a
//! test; the refusal on a server older than Redis 8.4 runs against one.
//!
//! ```text
//! REDIS_TEST_URL=redis://127.0.0.1:6379 cargo test --example redis_streams_claiming --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::time::Duration;

use ruststream_fred::stream::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
    in_stock: bool,
}

// --8<-- [start:claiming]
/// Fulfils new orders and the ones a worker took and never finished. An order out of stock stays
/// pending, and the subscription takes it back once it has been idle 30 seconds.
#[subscriber(RedisStream::claiming("orders", Duration::from_secs(30)).group("fulfilment"))]
async fn fulfil(order: &Order) -> HandlerOutcome {
    if !order.in_stock {
        tracing::warn!(order.id, "out of stock, the order comes back");
        return HandlerOutcome::retry();
    }
    tracing::info!(order.id, "order fulfilled");
    HandlerOutcome::ack()
}
// --8<-- [end:claiming]

fn app(url: &str) -> impl App {
    RustStream::new(AppInfo::new("fulfilment", "0.1.0")).with_broker(
        RedisBroker::standalone(url),
        |b| {
            b.include(fulfil);
        },
    )
}

// The scenarios are the tests below; the command in the header runs them.
fn main() {}

#[cfg(test)]
#[path = "../tests/live/mod.rs"]
mod live;

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;

    use super::*;

    /// The address the in-process run builds the broker with; nothing dials it.
    const URL: &str = "redis://localhost:6379";

    #[tokio::test(start_paused = true)]
    async fn a_retried_order_comes_back_once_it_has_been_idle_min_idle()
    -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app(URL)).await?;
        let broker = tb.broker::<RedisBroker>();

        broker
            .message(&Order {
                id: 1,
                in_stock: false,
            })
            .publish()
            .await?;
        broker
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::retry());

        // A retry appends nothing: the entry waits in the pending entries list.
        tb.advance(Duration::from_secs(29)).await?;
        broker.subscriber("orders").assert_called(1);
        broker.published::<Order>("orders").assert_called(1);

        tb.advance(Duration::from_secs(1)).await?;
        broker.subscriber("orders").assert_called(2).with(&Order {
            id: 1,
            in_stock: false,
        });
        tb.shutdown().await?;
        Ok(())
    }

    /// `XREADGROUP ... CLAIM` arrived in Redis 8.4, so an older server refuses the subscription
    /// and the service does not start.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_older_server_refuses_to_start_the_service() {
        let Some(url) = live::url("REDIS_TEST_URL") else {
            return;
        };

        let refused = TestApp::start_live(app(&url))
            .await
            .expect_err("Redis 7 has no XREADGROUP ... CLAIM");
        let refused = format!("{refused:?}");
        assert!(
            refused.contains("RedisStream::claiming") && refused.contains("8.4"),
            "the refusal names the mode and the version it needs: {refused}"
        );
    }
}
