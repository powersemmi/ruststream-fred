//! The service of `examples/redis_acknowledgement_out_retry.rs` written without the macros: the
//! handler implements `Handle`, and `out_retry(..).to(..)` follows the pattern's registration.
//!
//! ```text
//! cargo test --example manual_redis_acknowledgement_out_retry --features testing
//! cargo run --example manual_redis_acknowledgement_out_retry --features testing -- run
//! ```

use std::convert::Infallible;
use std::process::ExitCode;
use std::time::Duration;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::{self, JsonSchema};
use ruststream_fred::pubsub::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
struct Payment {
    id: u64,
    amount_cents: i64,
}

// A stand-in for a fraud-check client that times out on some payments: wired once at startup,
// read through `Context::state`.
#[derive(Clone)]
struct FraudCheck {
    endpoint: String,
}

impl FraudCheck {
    async fn screen(&self, payment: &Payment) -> Result<(), String> {
        tokio::task::yield_now().await;
        if payment.amount_cents % 10 == 7 {
            return Err(format!("{} timed out", self.endpoint));
        }
        Ok(())
    }
}

/// What the handlers share, built once at startup.
struct AppState {
    fraud: FraudCheck,
}

/// How long a payment the fraud check timed out on waits before it is screened again.
const RECHECK: Duration = Duration::from_secs(10);

/// Screens every payment announced on a `payments.*` channel.
struct Screen;

impl Handle<Payment, (), (), (), AppState> for Screen {
    async fn handle(
        &self,
        payment: &Payment,
        _outs: &(),
        ctx: &mut Context<'_, (), AppState>,
    ) -> Result<(), HandlerOutcome> {
        if let Err(err) = ctx.state().fraud.screen(payment).await {
            tracing::warn!(payment.id, %err, "screening failed, retrying later");
            return Err(HandlerOutcome::retry_after(RECHECK));
        }
        Ok(())
    }
}

fn startup() -> AppState {
    AppState {
        fraud: FraudCheck {
            endpoint: "https://fraud.internal".into(),
        },
    }
}

// --8<-- [start:app]
fn app() -> impl App<State = AppState> {
    RustStream::new(AppInfo::new("screening", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(startup()))
        .with_broker(RedisBroker::standalone("redis://localhost:6379"), |b| {
            // The copy is `PUBLISH`ed on `payments.retry`, which the pattern reads again.
            let payments = RedisPubSubPattern::new("payments.*");
            b.include(subscriber(payments, Screen).build())
                .out_retry(Publish::default())
                .to("payments.retry");
        })
}
// --8<-- [end:app]

fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::error::Error;

    use ruststream::runtime::RETRY_COUNT_HEADER;
    use ruststream::testing::TestApp;
    use ruststream_fred::pubsub::prelude::*;

    use super::{Payment, RECHECK, Screen, app, startup};

    /// The fraud check times out on every amount ending in 7.
    const TIMES_OUT: Payment = Payment {
        id: 7,
        amount_cents: 4_007,
    };

    #[tokio::test(start_paused = true)]
    async fn the_copy_goes_to_the_named_channel() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<RedisBroker>()
            .message(&TIMES_OUT)
            .to("payments.eu")
            .publish()
            .await?;

        tb.advance(RECHECK).await?;

        tb.broker::<RedisBroker>()
            .published::<Payment>("payments.retry")
            .assert_called_once()
            .with(&TIMES_OUT)
            .with_header(RETRY_COUNT_HEADER, "1");
        tb.broker::<RedisBroker>()
            .subscriber("payments.*")
            .assert_called(2)
            .with(&TIMES_OUT);
        Ok(())
    }

    #[tokio::test]
    async fn a_pattern_without_a_destination_does_not_start() {
        let unnamed = RustStream::new(AppInfo::new("screening", "0.1.0"))
            .on_startup(async move |()| Ok::<_, Infallible>(startup()))
            .with_broker(RedisBroker::standalone("redis://localhost:6379"), |b| {
                let payments = RedisPubSubPattern::new("payments.*");
                b.include(subscriber(payments, Screen).build());
            });

        let refused = TestApp::start(unnamed)
            .await
            .expect_err("a glob names no channel a copy could go to");
        assert!(
            refused.to_string().contains("names no destination"),
            "{refused}"
        );
    }
}
