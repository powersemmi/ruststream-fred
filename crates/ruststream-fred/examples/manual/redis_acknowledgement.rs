//! The service of `examples/redis_acknowledgement.rs` written without the macros: the handler
//! implements `Handle` and returns its outcome as the error of its verdict.
//!
//! ```text
//! cargo test --example manual_redis_acknowledgement --features testing
//! cargo run --example manual_redis_acknowledgement --features testing -- run
//! ```

use std::convert::Infallible;
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use ruststream::runtime::cli::run_main;
use ruststream::schemars::{self, JsonSchema};
use ruststream_fred::stream::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[outgoing(name = "payments")]
struct Payment {
    id: u64,
    amount_cents: i64,
}

// A stand-in for a payment gateway client: it is busy on the first charge it is asked for and
// takes every charge after that. Wired once at startup, read through `Context::state`.
#[derive(Clone, Default)]
struct PaymentGateway {
    ready: Arc<AtomicBool>,
}

impl PaymentGateway {
    async fn charge(&self, payment: &Payment) -> Result<(), String> {
        tokio::task::yield_now().await;
        if self.ready.swap(true, Ordering::Relaxed) {
            Ok(())
        } else {
            Err(format!("gateway busy, payment {}", payment.id))
        }
    }
}

/// What the handlers share, built once at startup.
struct AppState {
    gateway: PaymentGateway,
}

// --8<-- [start:outcomes]
/// One handler, three outcomes: the ack settles the entry, the drop settles it without a copy,
/// the retry appends a copy to `payments` and settles the original.
struct Charge;

impl Handle<Payment, (), (), (), AppState> for Charge {
    async fn handle(
        &self,
        payment: &Payment,
        _outs: &(),
        ctx: &mut Context<'_, (), AppState>,
    ) -> Result<(), HandlerOutcome> {
        if payment.amount_cents <= 0 {
            tracing::warn!(payment.id, "malformed payment dropped");
            return Err(HandlerOutcome::drop());
        }
        if let Err(err) = ctx.state().gateway.charge(payment).await {
            tracing::warn!(payment.id, %err, "charge failed, retrying");
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}

fn payments() -> RedisStream {
    RedisStream::new("payments").group("billing")
}
// --8<-- [end:outcomes]

fn app() -> impl App<State = AppState> {
    RustStream::new(AppInfo::new("billing", "0.1.0"))
        .on_startup(async move |()| {
            Ok::<_, Infallible>(AppState {
                gateway: PaymentGateway::default(),
            })
        })
        .with_broker(RedisBroker::standalone("redis://localhost:6379"), |b| {
            b.include(subscriber(payments(), Charge).build());
        })
}

fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::{Outcome, TestApp};
    use ruststream_fred::stream::prelude::*;

    use super::{Payment, app};

    const PAYMENT: Payment = Payment {
        id: 1,
        amount_cents: 2_500,
    };

    #[tokio::test]
    async fn a_retry_appends_a_copy_that_is_charged() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<RedisBroker>()
            .message(&PAYMENT)
            .publish()
            .await?;

        let redis = tb.broker::<RedisBroker>();
        assert_eq!(
            redis.subscriber("payments").outcomes(),
            [Outcome::Nack, Outcome::Ack]
        );
        // The copy is a second entry on the stream.
        redis
            .published::<Payment>("payments")
            .assert_called(2)
            .with(&PAYMENT);
        Ok(())
    }

    #[tokio::test]
    async fn a_dropped_payment_is_settled_without_a_copy() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let malformed = Payment {
            id: 2,
            amount_cents: 0,
        };
        tb.broker::<RedisBroker>()
            .message(&malformed)
            .publish()
            .await?;

        let redis = tb.broker::<RedisBroker>();
        assert_eq!(redis.subscriber("payments").outcomes(), [Outcome::Drop]);
        redis.published::<Payment>("payments").assert_called_once();
        Ok(())
    }
}
