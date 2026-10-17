//! A reply whose mount names no policy leaves through `RedisDefaultPublish`, which writes each
//! name the way this service reads it: here an `LPUSH` onto the list the job worker pops.
//!
//! ```text
//! cargo test --example redis_publishing_default --features testing
//! cargo run --example redis_publishing_default -- run
//! ```

use ruststream_fred::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "jobs")]
struct Job {
    order_id: u64,
}

// --8<-- [start:default]
/// Plans a job per order; the reply names no policy.
#[subscriber(RedisStream::new("orders").group("planners"), reply)]
async fn plan(order: &Order) -> Job {
    Job { order_id: order.id }
}

/// Pops the jobs off the `jobs` list.
#[subscriber(RedisList::new("jobs").reliable())]
async fn work(job: &Job) -> HandlerOutcome {
    tracing::info!(job.order_id, "job done");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("jobs", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            // `jobs` is a list this service reads, so the default publisher pushes onto it.
            b.include(plan);
            b.include(work);
        },
    )
}
// --8<-- [end:default]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_fred::prelude::*;

    use super::{Job, Order, app};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_planned_job_reaches_the_list_worker() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<RedisBroker>()
            .message(&Order { id: 21 })
            .publish()
            .await?;
        tb.settle().await?;

        tb.broker::<RedisBroker>()
            .subscriber("jobs")
            .assert_called_once()
            .with(&Job { order_id: 21 })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
}
