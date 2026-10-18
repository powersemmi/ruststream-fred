//! The service of `examples/redis_transactions.rs` written without `#[subscriber]`: a batch body
//! returns its replies, and the mount commits them as one block.
//!
//! ```text
//! cargo test --example manual_redis_transactions --features testing
//! cargo run --example manual_redis_transactions -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_fred::stream::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "processed")]
struct Processed {
    order_id: u64,
}

// --8<-- [start:transactional]
/// Processes a batch; subscribers of `processed` see all of its replies or none.
struct Process;

impl Handle<[Order], Vec<Processed>> for Process {
    fn handle(
        &self,
        orders: &[Order],
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Vec<Processed>, Vec<HandlerOutcome>>> {
        tracing::info!(orders = orders.len(), "batch processed");
        ready(Ok(orders
            .iter()
            .map(|order| Processed { order_id: order.id })
            .collect()))
    }
}

fn app() -> impl App {
    let broker = RedisBroker::standalone("redis://localhost:6379");
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        let process = subscriber(RedisStream::new("orders").group("workers"), Process);
        b.include(process.batch(nonzero!(32)).reply().build())
            .out_reply(TransactionalPublish)
            .transactional();
    })
}
// --8<-- [end:transactional]

fn main() -> ExitCode {
    run_main(app)
}

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
