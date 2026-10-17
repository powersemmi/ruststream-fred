//! A subscription by key: `#[subscriber("orders")]` reads the stream `orders` through the
//! broker's default consumer group. Each scenario is a test.
//!
//! ```text
//! cargo test --example redis_subscriptions_name --features testing
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

// --8<-- [start:name]
/// Reads the stream `orders`; the consumer group comes from the broker.
#[subscriber("orders")]
async fn place(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order placed");
    HandlerOutcome::ack()
}

fn app() -> impl App {
    let broker = RedisBroker::standalone("redis://localhost:6379").default_group("workers");
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(place);
    })
}
// --8<-- [end:name]

// The scenarios are the tests below; the command in the header runs them.
fn main() {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;

    use super::*;

    #[tokio::test]
    async fn the_stream_is_read_in_the_default_group() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<RedisBroker>()
            .message(&Order { id: 7 })
            .publish()
            .await?;

        tb.broker::<RedisBroker>()
            .subscriber("orders")
            .assert_called_once()
            .with(&Order { id: 7 })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_service_with_no_default_group_does_not_start() {
        let groupless = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
            RedisBroker::standalone("redis://localhost:6379"),
            |b| {
                b.include(place);
            },
        );

        let refused = TestApp::start(groupless)
            .await
            .expect_err("a stream is read only through a consumer group");
        let refused = format!("{refused:?}");
        assert!(
            refused.contains("default_group") && refused.contains("`orders`"),
            "the refusal names the setting and the key: {refused}"
        );
    }
}
