//! The service and tests of `examples/redis_testing_live.rs` without the macros: the handler is a
//! named type and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_redis_testing_live --features testing
//! just brokers-up
//! REDIS_TEST_URL=redis://127.0.0.1:6379 RUSTSTREAM_REQUIRE_LIVE=1 \
//!     cargo test --example manual_redis_testing_live --features testing
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
struct Payment {
    amount: u64,
}

// `#[outgoing(name = "testing-live.payments")]` by hand: a payment names its stream.
impl OutgoingDestination for Payment {
    type Form = FixedName;
    const DESTINATION: &'static str = "testing-live.payments";
}

impl MessageHeaders for Payment {
    type Contract = NoHeaders;
}

struct Accept;

impl Handle<Payment> for Accept {
    fn handle(
        &self,
        payment: &Payment,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        if payment.amount == 0 {
            return ready(Err(HandlerOutcome::drop()));
        }
        ready(Ok(()))
    }
}

/// The service on the server at `url`.
fn app(url: &str) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(
        RedisBroker::standalone(url),
        |b| {
            b.include(
                subscriber(
                    RedisStream::new("testing-live.payments").group("workers"),
                    Accept,
                )
                .build(),
            );
        },
    )
}

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(|| app("redis://localhost:6379"))
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_fred::stream::prelude::*;

    use super::{Payment, app, live};

    // --8<-- [start:start-live]
    /// The body both modes run; only the start call differs.
    async fn accepts_a_payment(tb: TestApp<()>) -> Result<(), Box<dyn Error>> {
        tb.broker::<RedisBroker>()
            .message(&Payment { amount: 100 })
            .publish()
            .await?;

        tb.broker::<RedisBroker>()
            .subscriber("testing-live.payments")
            .assert_called_once()
            .with(&Payment { amount: 100 })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn in_process() -> Result<(), Box<dyn Error>> {
        accepts_a_payment(TestApp::start(app("redis://localhost:6379")).await?).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn against_the_server() -> Result<(), Box<dyn Error>> {
        let Some(url) = live::url("REDIS_TEST_URL") else {
            return Ok(());
        };
        accepts_a_payment(TestApp::start_live(app(&url)).await?).await
    }
    // --8<-- [end:start-live]
}
