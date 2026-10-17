//! A broker over a `fred` pool the service built itself, for client settings the broker's own
//! builders do not reach. The in-process test runs everywhere; the live one runs against the
//! stand and skips without one.
//!
//! ```text
//! cargo test --example redis_broker_from_pool --features testing
//! just brokers-up
//! REDIS_TEST_URL=redis://127.0.0.1:6379 cargo test --example redis_broker_from_pool --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::time::Duration;

use fred::clients::Pool;
use fred::error::Error as FredError;
use fred::types::Builder;
use fred::types::config::{Config, ReconnectPolicy};
use serde::{Deserialize, Serialize};

use ruststream_fred::stream::prelude::*;

#[cfg(test)]
#[path = "../tests/live/mod.rs"]
mod live;

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "pooled.orders")]
struct Order {
    id: u64,
}

#[subscriber(RedisStream::new("pooled.orders").group("workers"))]
async fn take(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order taken");
    HandlerOutcome::ack()
}

fn service(broker: RedisBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(take);
    })
}

// --8<-- [start:pool]
/// Eight connections, a two-second bound on every command, and a reconnect every half second.
fn pool(url: &str) -> Result<Pool, FredError> {
    Builder::from_config(Config::from_url(url)?)
        .with_performance_config(|performance| {
            performance.default_command_timeout = Duration::from_secs(2);
        })
        .set_policy(ReconnectPolicy::new_constant(0, 500))
        .build_pool(8)
}

#[ruststream::app]
fn app() -> impl App<State = ()> {
    // A constant URL and a non-zero size: the pool always builds.
    let pool = pool("redis://localhost:6379").expect("a pool over a valid URL");
    // The pool is not connected here: the broker dials it when the service starts.
    service(RedisBroker::from_pool(pool))
}
// --8<-- [end:pool]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_fred::stream::prelude::*;

    use super::{Order, app, live, pool, service};

    /// The body both modes run.
    async fn takes_an_order(tb: TestApp<()>) -> Result<(), Box<dyn Error>> {
        tb.broker::<RedisBroker>()
            .message(&Order { id: 5 })
            .publish()
            .await?;

        tb.broker::<RedisBroker>()
            .subscriber("pooled.orders")
            .assert_called_once()
            .with(&Order { id: 5 })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn in_process() -> Result<(), Box<dyn Error>> {
        // The in-process mode reads the pool's size and settings and dials nothing.
        takes_an_order(TestApp::start(app()).await?).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn against_the_stand() -> Result<(), Box<dyn Error>> {
        let Some(url) = live::url("REDIS_TEST_URL") else {
            return Ok(());
        };
        // The pool is handed over unconnected, as the app builds it: the broker dials it at startup.
        let broker = RedisBroker::from_pool(pool(&url)?);
        takes_an_order(TestApp::start_live(service(broker)).await?).await
    }
}
