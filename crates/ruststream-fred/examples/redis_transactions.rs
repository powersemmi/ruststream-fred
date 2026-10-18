//! A batch handler whose replies commit together: `.transactional()` after the reply policy
//! buffers every reply of a batch and sends them as one `MULTI` / `EXEC` block.
//!
//! ```text
//! cargo test --example redis_transactions --features testing
//! cargo run --example redis_transactions -- run
//! ```

use ruststream_fred::stream::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "processed")]
struct Processed {
    order_id: u64,
}

// --8<-- [start:transactional]
/// Processes a batch; subscribers of `processed` see all of its replies or none.
#[subscriber(RedisStream::new("orders").group("workers"), reply)]
async fn process(orders: &[Order]) -> Result<Vec<Processed>, HandlerOutcome> {
    tracing::info!(orders = orders.len(), "batch processed");
    Ok(orders
        .iter()
        .map(|order| Processed { order_id: order.id })
        .collect())
}

#[ruststream::app]
fn app() -> impl App {
    let broker = RedisBroker::standalone("redis://localhost:6379");
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(process.batch(nonzero!(32)))
            .out_reply(TransactionalPublish)
            .transactional();
    })
}
// --8<-- [end:transactional]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_fred::stream::prelude::*;

    use super::{Order, Processed, app};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_batch_commits_every_reply() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        for id in 1..=3 {
            tb.broker::<RedisBroker>()
                .message(&Order { id })
                .publish()
                .await?;
        }
        tb.settle().await?;

        let processed: Vec<u64> = tb
            .broker::<RedisBroker>()
            .published::<Processed>("processed")
            .assert_called(3)
            .decoded()
            .into_iter()
            .map(|processed| processed.order_id)
            .collect();
        assert_eq!(processed, [1, 2, 3]);
        tb.shutdown().await?;
        Ok(())
    }
}
