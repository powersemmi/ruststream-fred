//! Where a retry copy goes: a pattern subscription reads many channels and names none, so its
//! registration names the channel with `out_retry`.
//!
//! ```text
//! cargo test --example redis_acknowledgement_out_retry --features testing
//! cargo run --example redis_acknowledgement_out_retry --features testing -- run
//! ```

use std::convert::Infallible;
use std::time::Duration;

use ruststream_fred::pubsub::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Payment {
    id: u64,
    amount_cents: i64,
}

// A stand-in for a fraud-check client that times out on some payments: wired once at startup,
// injected into handlers through `State`.
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

// `#[derive(FromRef)]` makes each state field injectable with `State<FieldType>`.
#[derive(FromRef)]
struct AppState {
    fraud: FraudCheck,
}

/// How long a payment the fraud check timed out on waits before it is screened again.
const RECHECK: Duration = Duration::from_secs(10);

/// Screens every payment announced on a `payments.*` channel.
#[subscriber(RedisPubSubPattern::new("payments.*"))]
async fn screen(payment: &Payment, State(fraud): State<FraudCheck>) -> HandlerOutcome {
    match fraud.screen(payment).await {
        Ok(()) => HandlerOutcome::ack(),
        Err(err) => {
            tracing::warn!(payment.id, %err, "screening failed, retrying later");
            HandlerOutcome::retry_after(RECHECK)
        }
    }
}

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App<State = AppState> {
    RustStream::new(AppInfo::new("screening", "0.1.0"))
        .on_startup(async move |()| {
            let fraud = FraudCheck {
                endpoint: "https://fraud.internal".into(),
            };
            Ok::<_, Infallible>(AppState { fraud })
        })
        .with_broker(RedisBroker::standalone("redis://localhost:6379"), |b| {
            // The copy is `PUBLISH`ed on `payments.retry`, which the pattern reads again.
            b.include(screen)
                .out_retry(Publish::default())
                .to("payments.retry");
        })
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::error::Error;

    use ruststream::runtime::RETRY_COUNT_HEADER;
    use ruststream::testing::TestApp;
    use ruststream_fred::pubsub::prelude::*;

    use super::{AppState, FraudCheck, Payment, RECHECK, app, screen};

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
            .on_startup(async move |()| {
                let fraud = FraudCheck {
                    endpoint: "https://fraud.internal".into(),
                };
                Ok::<_, Infallible>(AppState { fraud })
            })
            .with_broker(RedisBroker::standalone("redis://localhost:6379"), |b| {
                b.include(screen);
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
