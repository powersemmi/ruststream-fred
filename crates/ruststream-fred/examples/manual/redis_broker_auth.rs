//! The service of `examples/redis_broker_auth.rs` without the macros: the handler is a named type
//! and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_redis_broker_auth --features testing
//! just brokers-up
//! REDIS_AUTH_TEST_URL=redis://127.0.0.1:6385 cargo test --example manual_redis_broker_auth --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::env;
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

// `#[outgoing(name = "auth.orders")]` by hand: an order names the stream it is added to.
impl OutgoingDestination for Order {
    type Form = FixedName;
    const DESTINATION: &'static str = "auth.orders";
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
        b.include(subscriber(RedisStream::new("auth.orders").group("workers"), Take).build());
    })
}

// --8<-- [start:broker]
/// The ACL user `worker`; its password comes from the environment, never from the source.
fn broker(url: &str, password: &str) -> RedisBroker {
    RedisBroker::standalone(url).credentials("worker", password)
}

fn app() -> impl App<State = ()> {
    let password = env::var("REDIS_PASSWORD").unwrap_or_default();
    service(broker("redis://localhost:6379", &password))
}
// --8<-- [end:broker]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_fred::stream::prelude::*;

    use super::{Order, app, broker, live, service};

    /// The body every mode runs.
    async fn takes_an_order(tb: TestApp<()>) -> Result<(), Box<dyn Error>> {
        tb.broker::<RedisBroker>()
            .message(&Order { id: 9 })
            .publish()
            .await?;

        tb.broker::<RedisBroker>()
            .subscriber("auth.orders")
            .assert_called_once()
            .with(&Order { id: 9 })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn in_process() -> Result<(), Box<dyn Error>> {
        takes_an_order(TestApp::start(app()).await?).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_acl_user_authenticates() -> Result<(), Box<dyn Error>> {
        let Some(url) = live::url("REDIS_AUTH_TEST_URL") else {
            return Ok(());
        };
        takes_an_order(TestApp::start_live(service(broker(&url, "workerpass"))).await?).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_password_alone_authenticates_the_default_user() -> Result<(), Box<dyn Error>> {
        let Some(url) = live::url("REDIS_AUTH_TEST_URL") else {
            return Ok(());
        };
        let broker = RedisBroker::standalone(url).password("s3cr3t");
        takes_an_order(TestApp::start_live(service(broker)).await?).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_wrong_password_does_not_start() {
        let Some(url) = live::url("REDIS_AUTH_TEST_URL") else {
            return;
        };

        assert!(
            TestApp::start_live(service(broker(&url, "not-the-password")))
                .await
                .is_err()
        );
    }
}
