//! Delayed retries on Redis streams: the copy the runtime publishes once the delay is up, and the
//! sorted-set delay queue that keeps the same retry in Redis.
//!
//! ```text
//! cargo test --example redis_acknowledgement_retries --features testing
//! cargo run --example redis_acknowledgement_retries --features testing -- run
//! ```

use std::convert::Infallible;
use std::time::Duration;

use ruststream_fred::stream::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Payment {
    id: u64,
    amount_cents: i64,
}

// A stand-in for a payment gateway client that times out on some payments: wired once at
// startup, injected into handlers through `State`.
#[derive(Clone)]
struct PaymentGateway {
    endpoint: String,
}

impl PaymentGateway {
    async fn charge(&self, payment: &Payment) -> Result<(), String> {
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
    gateway: PaymentGateway,
}

/// How long a payment the gateway timed out on waits before it is charged again.
const RECHECK: Duration = Duration::from_secs(30);

// --8<-- [start:retry_after]
/// A timed-out charge comes back after `RECHECK`: the runtime holds a copy and publishes it to
/// `card-payments` once the delay is up.
#[subscriber(RedisStream::new("card-payments").group("billing"))]
async fn charge_card(payment: &Payment, State(gateway): State<PaymentGateway>) -> HandlerOutcome {
    match gateway.charge(payment).await {
        Ok(()) => HandlerOutcome::ack(),
        Err(err) => {
            tracing::warn!(payment.id, %err, "charge failed, retrying later");
            HandlerOutcome::retry_after(RECHECK)
        }
    }
}
// --8<-- [end:retry_after]

// --8<-- [start:delayed_retry]
/// The delay waits in the `bank-payments.delayed` sorted set in Redis, and the subscription puts
/// the entry back on `bank-payments` once it is due.
#[subscriber(
    RedisStream::new("bank-payments")
        .group("billing")
        .delayed_retry(DelayedRetry::DurableZset {
            key: "bank-payments.delayed".to_owned(),
            ttl: None,
        })
)]
async fn charge_bank(payment: &Payment, State(gateway): State<PaymentGateway>) -> HandlerOutcome {
    match gateway.charge(payment).await {
        Ok(()) => HandlerOutcome::ack(),
        Err(err) => {
            tracing::warn!(payment.id, %err, "charge failed, parked for later");
            HandlerOutcome::retry_after(RECHECK)
        }
    }
}
// --8<-- [end:delayed_retry]

#[ruststream::app]
fn app() -> impl App<State = AppState> {
    RustStream::new(AppInfo::new("billing", "0.1.0"))
        .on_startup(async move |()| {
            let gateway = PaymentGateway {
                endpoint: "https://gateway.internal".into(),
            };
            Ok::<_, Infallible>(AppState { gateway })
        })
        .with_broker(RedisBroker::standalone("redis://localhost:6379"), |b| {
            b.include(charge_card);
            b.include(charge_bank);
        })
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::runtime::RETRY_COUNT_HEADER;
    use ruststream::testing::TestApp;
    use ruststream_fred::stream::prelude::*;

    use super::{Payment, RECHECK, app};

    /// The gateway times out on every amount ending in 7.
    const TIMES_OUT: Payment = Payment {
        id: 7,
        amount_cents: 1_007,
    };

    #[tokio::test(start_paused = true)]
    async fn the_runtime_publishes_a_copy_after_the_delay() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<RedisBroker>()
            .message(&TIMES_OUT)
            .to("card-payments")
            .publish()
            .await?;
        tb.broker::<RedisBroker>()
            .subscriber("card-payments")
            .assert_called_once()
            .settled(HandlerOutcome::retry_after(RECHECK));

        tb.advance(RECHECK).await?;

        tb.broker::<RedisBroker>()
            .subscriber("card-payments")
            .assert_called(2)
            .with(&TIMES_OUT);
        tb.broker::<RedisBroker>()
            .published::<Payment>("card-payments")
            .assert_called(2)
            .with_header(RETRY_COUNT_HEADER, "1");
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_parked_payment_returns_when_it_is_due() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<RedisBroker>()
            .message(&TIMES_OUT)
            .to("bank-payments")
            .publish()
            .await?;

        // Half the delay: the entry waits in the sorted set, and the stream holds nothing new.
        tb.advance(RECHECK / 2).await?;
        tb.broker::<RedisBroker>()
            .subscriber("bank-payments")
            .assert_called_once();
        tb.broker::<RedisBroker>()
            .published::<Payment>("bank-payments")
            .assert_called_once();

        tb.advance(RECHECK / 2).await?;
        tb.broker::<RedisBroker>()
            .subscriber("bank-payments")
            .assert_called(2)
            .with(&TIMES_OUT);
        tb.broker::<RedisBroker>()
            .published::<Payment>("bank-payments")
            .assert_called(2)
            .with_header(RETRY_COUNT_HEADER, "1");
        Ok(())
    }
}
