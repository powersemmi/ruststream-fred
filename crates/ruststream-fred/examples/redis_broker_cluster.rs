//! A service on a Redis Cluster: `RedisBroker::cluster` takes seed nodes, and the client finds
//! the rest of the cluster from them. The in-process test runs everywhere; the live one runs
//! against the stand's cluster and skips without one.
//!
//! ```text
//! cargo test --example redis_broker_cluster --features testing
//! just brokers-up
//! REDIS_CLUSTER_TEST_URL=127.0.0.1:7000 cargo test --example redis_broker_cluster --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use serde::{Deserialize, Serialize};

use ruststream_fred::list::prelude::*;

#[cfg(test)]
#[path = "../tests/live/mod.rs"]
mod live;

// A hash-tagged key reads as a name template in `#[outgoing(name)]`, so the test names it.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Job {
    id: u64,
}

// --8<-- [start:app]
/// The hash tag `{jobs}` keeps the queue and its processing list `{jobs}.processing` on one node.
#[subscriber(RedisList::new("{jobs}").reliable())]
async fn run_job(job: &Job) -> HandlerOutcome {
    tracing::info!(job.id, "job done");
    HandlerOutcome::ack()
}

fn service(broker: RedisBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("jobs", "0.1.0")).with_broker(broker, |b| {
        b.include(run_job);
    })
}

/// Two seeds: the client reaches the cluster through whichever answers first.
#[ruststream::app]
fn app() -> impl App<State = ()> {
    service(RedisBroker::cluster([
        "redis-1.internal:6379",
        "redis-2.internal:6379",
    ]))
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_fred::list::prelude::*;

    use super::{Job, app, live, service};

    /// The body both modes run.
    async fn runs_a_job(tb: TestApp<()>) -> Result<(), Box<dyn Error>> {
        tb.broker::<RedisBroker>()
            .message(&Job { id: 3 })
            .to("{jobs}")
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

    #[subscriber(RedisList::new("jobs").reliable())]
    async fn untagged(job: &Job) -> HandlerOutcome {
        let _ = job.id;
        HandlerOutcome::ack()
    }

    #[tokio::test]
    async fn a_reliable_list_across_two_slots_does_not_start() {
        let app = RustStream::new(AppInfo::new("jobs", "0.1.0")).with_broker(
            RedisBroker::cluster(["redis-1.internal:6379"]),
            |b| {
                b.include(untagged);
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
