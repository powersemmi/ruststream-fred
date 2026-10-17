//! The service of `examples/redis_streams_reclaim.rs` written without the macros: the worker and
//! the recovery path are two `subscriber(..)` mounts on one consumer group.
//!
//! ```text
//! cargo test --example manual_redis_streams_reclaim --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::future::{Future, ready};
use std::time::Duration;

use ruststream::schemars::{self, JsonSchema};
use ruststream_fred::stream::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
struct Order {
    id: u64,
}

// `#[outgoing(name = "orders")]` by hand: the type fixes its destination.
impl OutgoingDestination for Order {
    type Form = FixedName;

    const DESTINATION: &'static str = "orders";
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

// --8<-- [start:reclaim]
/// The worker: fresh entries off the tail of `orders`.
struct Work;

impl Handle<Order> for Work {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, "order processed");
        ready(Ok(()))
    }
}

/// The recovery path: entries a worker took and left unacknowledged.
struct Recover;

impl Handle<Order> for Recover {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, "order recovered");
        ready(Ok(()))
    }
}

fn app() -> impl App {
    let broker = RedisBroker::standalone("redis://localhost:6379");
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        let fresh = RedisStream::new("orders").group("workers");
        b.include(subscriber(fresh, Work).build());
        // Idle a minute: the worker that took it crashed or hung.
        let stale = RedisStream::reclaim("orders", Duration::from_secs(60)).group("workers");
        b.include(subscriber(stale, Recover).build());
    })
}
// --8<-- [end:reclaim]

// The scenarios are the tests below; the command in the header runs them.
fn main() {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;

    use super::*;

    /// An entry the worker acknowledged leaves the pending entries list, so the recovery path
    /// never reads it, however long the clock runs.
    #[tokio::test(start_paused = true)]
    async fn an_acknowledged_entry_is_never_reclaimed() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<RedisBroker>();

        broker.message(&Order { id: 1 }).publish().await?;
        tb.advance(Duration::from_secs(120)).await?;

        // Both subscriptions read `orders`: the one delivery is the worker's.
        broker
            .subscriber("orders")
            .assert_called_once()
            .with(&Order { id: 1 })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    /// The recovery path alone on a group reads nothing off the tail: a fresh entry is no one's
    /// unfinished work.
    #[tokio::test(start_paused = true)]
    async fn the_recovery_path_reads_no_fresh_entry() -> Result<(), Box<dyn Error>> {
        let recovery_only = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
            RedisBroker::standalone("redis://localhost:6379"),
            |b| {
                let stale =
                    RedisStream::reclaim("orders", Duration::from_secs(60)).group("workers");
                b.include(subscriber(stale, Recover).build());
            },
        );
        let tb = TestApp::start(recovery_only).await?;
        let broker = tb.broker::<RedisBroker>();

        broker.message(&Order { id: 1 }).publish().await?;
        tb.advance(Duration::from_secs(120)).await?;

        broker.subscriber("orders").assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }
}
