//! One test body, two runs: in process with `TestApp::start`, and against a Redis server with
//! `TestApp::start_live`. The live run skips without a server.
//!
//! ```text
//! cargo test --example redis_testing_live --features testing
//! just brokers-up
//! REDIS_TEST_URL=redis://127.0.0.1:6379 RUSTSTREAM_REQUIRE_LIVE=1 \
//!     cargo test --example redis_testing_live --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use serde::{Deserialize, Serialize};

use ruststream_fred::stream::prelude::*;

#[cfg(test)]
#[path = "../tests/live/mod.rs"]
mod live;

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "testing-live.payments")]
struct Payment {
    amount: u64,
}

#[subscriber(RedisStream::new("testing-live.payments").group("workers"))]
async fn accept(payment: &Payment) -> HandlerOutcome {
    if payment.amount == 0 {
        return HandlerOutcome::drop();
    }
    HandlerOutcome::ack()
}

/// The service on the server at `url`.
fn app(url: &str) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(
        RedisBroker::standalone(url),
        |b| {
            b.include(accept);
        },
    )
}

#[ruststream::app]
fn service() -> impl App<State = ()> {
    app("redis://localhost:6379")
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
