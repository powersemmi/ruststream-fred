//! The service of `examples/redis_positions_start_at.rs` written without the macros:
//! `.start_at(position)` is a step of the mount chain after `build()`.
//!
//! ```text
//! REDIS_TEST_URL=redis://127.0.0.1:6379 cargo test --example manual_redis_positions_start_at --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::future::{Future, ready};

use ruststream::schemars::{self, JsonSchema};
use ruststream_fred::stream::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
struct AuditEntry {
    id: u64,
}

// `#[outgoing(name = "audit.trail")]` by hand: the type fixes its destination.
impl OutgoingDestination for AuditEntry {
    type Form = FixedName;

    const DESTINATION: &'static str = "audit.trail";
}

impl MessageHeaders for AuditEntry {
    type Contract = NoHeaders;
}

// --8<-- [start:start-at]
/// Projects every audit entry it reads.
struct Project;

impl Handle<AuditEntry> for Project {
    fn handle(
        &self,
        entry: &AuditEntry,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(entry.id, "entry projected");
        ready(Ok(()))
    }
}

fn app(url: &str) -> impl App {
    RustStream::new(AppInfo::new("audit", "0.1.0")).with_broker(RedisBroker::standalone(url), |b| {
        // The projection is rebuilt from the oldest retained entry on every start.
        let trail = RedisStream::new("audit.trail").group("projection");
        b.include(
            subscriber(trail, Project)
                .build()
                .start_at(RedisGroupPosition::beginning()),
        );
    })
}
// --8<-- [end:start-at]

// The scenarios are the tests below; the command in the header runs them.
fn main() {}

#[cfg(test)]
#[path = "../../tests/live/mod.rs"]
mod live;

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;

    use super::*;

    /// The address the in-process run builds the broker with; nothing dials it.
    const URL: &str = "redis://localhost:6379";

    #[tokio::test]
    async fn the_positioned_group_reads_new_entries() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app(URL)).await?;
        let broker = tb.broker::<RedisBroker>();

        broker.message(&AuditEntry { id: 1 }).publish().await?;

        broker
            .subscriber("audit.trail")
            .assert_called_once()
            .with(&AuditEntry { id: 1 })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    /// The second run starts at the beginning again, so the entry the first run acknowledged
    /// comes back before the one published after the restart.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn every_start_replays_the_retained_stream() -> Result<(), Box<dyn Error>> {
        let Some(url) = live::url("REDIS_TEST_URL") else {
            return Ok(());
        };

        let first = TestApp::start_live(app(&url)).await?;
        first
            .broker::<RedisBroker>()
            .message(&AuditEntry { id: 1 })
            .publish()
            .await?;
        first.shutdown().await?;

        let tb = TestApp::start_live(app(&url)).await?;
        tb.broker::<RedisBroker>()
            .message(&AuditEntry { id: 2 })
            .publish()
            .await?;

        // A live publish returns once the subscription has handled as many entries as the test
        // published, so the first of them is what decides: an entry the group had acknowledged.
        let projected = tb
            .broker::<RedisBroker>()
            .subscriber("audit.trail")
            .received::<AuditEntry>();
        assert_eq!(projected.first(), Some(&AuditEntry { id: 1 }));
        tb.shutdown().await?;
        Ok(())
    }
}
