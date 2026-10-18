//! The processing list of a reliable Redis list on a cluster: the queue and the list a claimed
//! entry waits on share one hash tag, so a claim moves the entry within one slot.
//!
//! ```text
//! cargo test --example redis_lists_processing --features testing
//! cargo run --example redis_lists_processing --features testing -- run
//! ```

use ruststream_fred::list::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Export {
    id: u64,
}

// --8<-- [start:processing]
/// `{exports}` is the hash tag of both keys, so the queue and its processing list live on one
/// slot of the cluster.
#[subscriber(
    RedisList::new("{exports}")
        .reliable()
        .processing("{exports}.in-flight")
)]
async fn export(job: &Export) -> HandlerOutcome {
    tracing::info!(job.id, "export written");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("exports", "0.1.0")).with_broker(
        RedisBroker::cluster(["redis://localhost:7000"]),
        |b| {
            b.include(export);
        },
    )
}
// --8<-- [end:processing]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_fred::list::prelude::*;

    use super::{Export, app};

    #[tokio::test]
    async fn a_tagged_queue_runs_on_a_cluster() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<RedisBroker>()
            .message(&Export { id: 4 })
            .to("{exports}")
            .publish()
            .await?;

        tb.broker::<RedisBroker>()
            .subscriber("{exports}")
            .assert_called_once()
            .with(&Export { id: 4 })
            .settled(HandlerOutcome::ack());
        Ok(())
    }

    #[subscriber(RedisList::new("exports").reliable().processing("exports.in-flight"))]
    async fn export_untagged(job: &Export) -> HandlerOutcome {
        tracing::info!(job.id, "export written");
        HandlerOutcome::ack()
    }

    #[tokio::test]
    async fn keys_on_two_slots_do_not_start() {
        let untagged = RustStream::new(AppInfo::new("exports", "0.1.0")).with_broker(
            RedisBroker::cluster(["redis://localhost:7000"]),
            |b| {
                b.include(export_untagged);
            },
        );

        let refused = TestApp::start(untagged)
            .await
            .expect_err("a claim cannot move an entry between two slots");
        assert!(refused.to_string().contains("hash tag"), "{refused}");
    }
}
