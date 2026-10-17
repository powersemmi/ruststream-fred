//! The service of `examples/redis_broker_cluster.rs` without the macros: the handler is a named
//! type and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_redis_broker_cluster --features testing
//! just brokers-up
//! REDIS_CLUSTER_TEST_URL=127.0.0.1:7000 cargo test --example manual_redis_broker_cluster --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream_fred::list::prelude::*;

#[cfg(test)]
#[path = "../../tests/live/mod.rs"]
mod live;

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Job {
    id: u64,
}

// `#[outgoing(name = "{jobs}")]` by hand: a job names the list it is pushed to.
impl OutgoingDestination for Job {
    type Form = FixedName;
    const DESTINATION: &'static str = "{jobs}";
}

impl MessageHeaders for Job {
    type Contract = NoHeaders;
}

// --8<-- [start:app]
struct RunJob;

impl Handle<Job> for RunJob {
    fn handle(
        &self,
        job: &Job,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(job.id, "job done");
        ready(Ok(()))
    }
}

/// The hash tag `{jobs}` keeps the queue and its processing list `{jobs}.processing` on one node.
fn service(broker: RedisBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("jobs", "0.1.0")).with_broker(broker, |b| {
        b.include(subscriber(RedisList::new("{jobs}").reliable(), RunJob).build());
    })
}

/// Two seeds: the client reaches the cluster through whichever answers first.
fn app() -> impl App<State = ()> {
    service(RedisBroker::cluster([
        "redis-1.internal:6379",
        "redis-2.internal:6379",
    ]))
}
// --8<-- [end:app]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_fred::list::prelude::*;

    use super::{Job, RunJob, app, live, service};

    /// The body both modes run.
    async fn runs_a_job(tb: TestApp<()>) -> Result<(), Box<dyn Error>> {
        tb.broker::<RedisBroker>()
            .message(&Job { id: 3 })
            .publish()
            .await?;

        tb.broker::<RedisBroker>()
            .subscriber("{jobs}")
            .assert_called_once()
            .with(&Job { id: 3 })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn in_process() -> Result<(), Box<dyn Error>> {
        runs_a_job(TestApp::start(app()).await?).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn against_the_stand() -> Result<(), Box<dyn Error>> {
        let Some(node) = live::url("REDIS_CLUSTER_TEST_URL") else {
            return Ok(());
        };
        // One seed: the client discovers the other nodes from it.
        runs_a_job(TestApp::start_live(service(RedisBroker::cluster([node]))).await?).await
    }

    #[tokio::test]
    async fn a_reliable_list_across_two_slots_does_not_start() {
        let app = RustStream::new(AppInfo::new("jobs", "0.1.0")).with_broker(
            RedisBroker::cluster(["redis-1.internal:6379"]),
            |b| {
                b.include(subscriber(RedisList::new("jobs").reliable(), RunJob).build());
            },
        );

        let err = TestApp::start(app)
            .await
            .err()
            .map(|err| format!("{err:?}"));
        assert!(
            err.as_deref().is_some_and(|err| err.contains("hash tag")),
            "{err:?}"
        );
    }
}
