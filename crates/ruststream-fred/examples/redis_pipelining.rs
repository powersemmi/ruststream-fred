//! A pipelined stream subscription: the handler queues its own Redis command into the delivery's
//! round, and the command leaves with the delivery's `XACK`, only when the delivery is
//! acknowledged.
//!
//! ```text
//! cargo test --example redis_pipelining --features testing
//! cargo run --example redis_pipelining -- run
//! ```

use ruststream_fred::stream::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
    items: u32,
}

// --8<-- [start:pipeline]
/// Records an accepted order on the `orders:accepted` list, in the same round trip as its `XACK`.
#[subscriber(RedisStream::new("orders").group("workers"))]
async fn accept(order: &Order, Ctx(pipeline): Ctx<keys::Pipeline>) -> HandlerOutcome {
    if pipeline
        .lpush("orders:accepted", order.id.to_string())
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    if order.items == 0 {
        // The delivery is dropped, and the push queued above goes with it.
        tracing::warn!(order.id, "an order without items is dropped");
        return HandlerOutcome::drop();
    }
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            b.include(accept.pipeline());
        },
    )
}
// --8<-- [end:pipeline]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_fred::stream::prelude::*;

    use super::{Order, app};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_acknowledged_order_leaves_its_push() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<RedisBroker>()
            .message(&Order { id: 7, items: 2 })
            .publish()
            .await?;

        tb.broker::<RedisBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.broker::<RedisBroker>()
            .published::<u64>("orders:accepted")
            .assert_called_once()
            .with(&7);
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_dropped_order_leaves_nothing_it_queued() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<RedisBroker>()
            .message(&Order { id: 8, items: 0 })
            .publish()
            .await?;

        tb.broker::<RedisBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.broker::<RedisBroker>()
            .published::<u64>("orders:accepted")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }
}
