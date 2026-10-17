//! The service of `examples/redis_broker_sentinel.rs` without the macros: the handler is a named
//! type and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_redis_broker_sentinel --features testing
//! just brokers-up
//! REDIS_SENTINEL_TEST_URL=127.0.0.1:26379 cargo test --example manual_redis_broker_sentinel --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream_fred::stream::prelude::*;

#[cfg(test)]
#[path = "../../tests/live/mod.rs"]
mod live;

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
}

// `#[outgoing(name = "sentinel.orders")]` by hand: an order names the stream it is added to.
impl OutgoingDestination for Order {
    type Form = FixedName;
    const DESTINATION: &'static str = "sentinel.orders";
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

struct Take;

impl Handle<Order> for Take {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, "order taken");
        ready(Ok(()))
    }
}

fn service(broker: RedisBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(subscriber(RedisStream::new("sentinel.orders").group("workers"), Take).build());
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

fn app() -> impl App<State = ()> {
    service(broker())
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
