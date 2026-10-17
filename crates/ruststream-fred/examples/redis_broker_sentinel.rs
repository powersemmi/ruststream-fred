//! A service on a Sentinel deployment: `RedisBroker::sentinel` names the monitored primary and the
//! sentinels that watch it. The in-process test runs everywhere; the live one runs against the
//! stand's sentinel and skips without one.
//!
//! ```text
//! cargo test --example redis_broker_sentinel --features testing
//! just brokers-up
//! REDIS_SENTINEL_TEST_URL=127.0.0.1:26379 cargo test --example redis_broker_sentinel --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use serde::{Deserialize, Serialize};

use ruststream_fred::stream::prelude::*;

#[cfg(test)]
#[path = "../tests/live/mod.rs"]
mod live;

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "sentinel.orders")]
struct Order {
    id: u64,
}

#[subscriber(RedisStream::new("sentinel.orders").group("workers"))]
async fn take(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order taken");
    HandlerOutcome::ack()
}

fn service(broker: RedisBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(take);
    })
}

// --8<-- [start:app]
/// The primary `mymaster`, found through either of two sentinels.
fn broker() -> RedisBroker {
    RedisBroker::sentinel(
        "mymaster",
        ["sentinel-1.internal:26379", "sentinel-2.internal:26379"],
    )
}

#[ruststream::app]
fn app() -> impl App<State = ()> {
    service(broker())
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_fred::stream::prelude::*;

    use super::{Order, app, live, service};

    /// The body both modes run.
    async fn takes_an_order(tb: TestApp<()>) -> Result<(), Box<dyn Error>> {
        tb.broker::<RedisBroker>()
            .message(&Order { id: 11 })
            .publish()
            .await?;

        tb.broker::<RedisBroker>()
            .subscriber("sentinel.orders")
            .assert_called_once()
            .with(&Order { id: 11 })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn in_process() -> Result<(), Box<dyn Error>> {
        takes_an_order(TestApp::start(app()).await?).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn against_the_stand() -> Result<(), Box<dyn Error>> {
        let Some(sentinel) = live::url("REDIS_SENTINEL_TEST_URL") else {
            return Ok(());
        };
        let broker = RedisBroker::sentinel("mymaster", [sentinel]);
        takes_an_order(TestApp::start_live(service(broker)).await?).await
    }
}
