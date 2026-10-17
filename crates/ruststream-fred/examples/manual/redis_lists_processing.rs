//! The service of `examples/redis_lists_processing.rs` written without the macros: the handler
//! implements `Handle`, and `subscriber(descriptor, handler)` mounts it on the tagged queue.
//!
//! ```text
//! cargo test --example manual_redis_lists_processing --features testing
//! cargo run --example manual_redis_lists_processing --features testing -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::{self, JsonSchema};
use ruststream_fred::list::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
struct Export {
    id: u64,
}

/// Writes one export.
struct WriteExport;

impl Handle<Export> for WriteExport {
    fn handle(
        &self,
        job: &Export,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(job.id, "export written");
        ready(Ok(()))
    }
}

// --8<-- [start:processing]
/// `{exports}` is the hash tag of both keys, so the queue and its processing list live on one
/// slot of the cluster.
fn exports() -> RedisList {
    RedisList::new("{exports}")
        .reliable()
        .processing("{exports}.in-flight")
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("exports", "0.1.0")).with_broker(
        RedisBroker::cluster(["redis://localhost:7000"]),
        |b| {
            b.include(subscriber(exports(), WriteExport).build());
        },
    )
}
// --8<-- [end:processing]

fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_fred::list::prelude::*;

    use super::{Export, WriteExport, app};

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

    #[tokio::test]
    async fn keys_on_two_slots_do_not_start() {
        let untagged = RustStream::new(AppInfo::new("exports", "0.1.0")).with_broker(
            RedisBroker::cluster(["redis://localhost:7000"]),
            |b| {
                let descriptor = RedisList::new("exports")
                    .reliable()
                    .processing("exports.in-flight");
                b.include(subscriber(descriptor, WriteExport).build());
            },
        );

        let refused = TestApp::start(untagged)
            .await
            .expect_err("a claim cannot move an entry between two slots");
        assert!(refused.to_string().contains("hash tag"), "{refused}");
    }
}
