//! The service of `examples/redis_publishing_default.rs` written without `#[subscriber]`: a reply
//! registration that names no policy gets the default one.
//!
//! ```text
//! cargo test --example manual_redis_publishing_default --features testing
//! cargo run --example manual_redis_publishing_default -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_fred::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "jobs")]
struct Job {
    order_id: u64,
}

// --8<-- [start:default]
/// Plans a job per order; the reply names no policy.
struct Plan;

impl Handle<Order, Job> for Plan {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Job, HandlerOutcome>> {
        ready(Ok(Job { order_id: order.id }))
    }
}

/// Pops the jobs off the `jobs` list.
struct Work;

impl Handle<Job> for Work {
    fn handle(
        &self,
        job: &Job,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(job.order_id, "job done");
        ready(Ok(()))
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("jobs", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            let plan = subscriber(RedisStream::new("orders").group("planners"), Plan);
            // `jobs` is a list this service reads, so the default publisher pushes onto it.
            b.include(plan.reply().build());
            b.include(subscriber(RedisList::new("jobs").reliable(), Work).build());
        },
    )
}
// --8<-- [end:default]

fn main() -> ExitCode {
    run_main(app)
}

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
