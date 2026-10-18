//! The broker: `RedisBroker::standalone` records the server's URL, the runtime connects it at
//! startup, and shuts it down when the service stops. The test runs the same app in process.
//!
//! ```text
//! cargo run --example redis_broker --features testing -- run
//! cargo test --example redis_broker --features testing
//! ```

use serde::{Deserialize, Serialize};

use ruststream_fred::stream::prelude::*;

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

// --8<-- [start:app]
#[subscriber(RedisStream::new("orders").group("workers"))]
async fn take(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order taken");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App<State = ()> {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            b.include(take);
        },
    )
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_fred::stream::prelude::*;

    use super::{Order, app};

    #[tokio::test]
    async fn the_service_takes_an_order() -> Result<(), Box<dyn Error>> {
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
}
