//! The service of `examples/redis_acknowledgement_dead_letter.rs` written without the macros:
//! each handler implements `Handle`, and the cap and the dead-letter destination are the same
//! steps of the mount chain.
//!
//! ```text
//! cargo test --example manual_redis_acknowledgement_dead_letter --features testing
//! cargo run --example manual_redis_acknowledgement_dead_letter --features testing -- run
//! ```

use std::convert::Infallible;
use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::RETRY_COUNT_HEADER;
use ruststream::runtime::cli::run_main;
use ruststream::schemars::{self, JsonSchema};
use ruststream_fred::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
struct Payment {
    id: u64,
    amount_cents: i64,
}

// A stand-in for a payment gateway client that rejects some payments every time: wired once at
// startup, read through `Context::state`.
#[derive(Clone)]
struct PaymentGateway {
    endpoint: String,
}

impl PaymentGateway {
    async fn transfer(&self, payment: &Payment) -> Result<(), String> {
        tokio::task::yield_now().await;
        if payment.amount_cents % 10 == 7 {
            return Err(format!("{} rejected the transfer", self.endpoint));
        }
        Ok(())
    }
}

/// What the handlers share, built once at startup.
struct AppState {
    gateway: PaymentGateway,
}

/// A failed charge asks for a retry; the handler counts nothing itself.
struct Charge;

impl Handle<Payment, (), (), (), AppState> for Charge {
    async fn handle(
        &self,
        payment: &Payment,
        _outs: &(),
        ctx: &mut Context<'_, (), AppState>,
    ) -> Result<(), HandlerOutcome> {
        if let Err(err) = ctx.state().gateway.transfer(payment).await {
            tracing::warn!(payment.id, %err, "charge failed, retrying");
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}

/// The same on a reliable list of refunds.
struct Refund;

impl Handle<Payment, (), (), (), AppState> for Refund {
    async fn handle(
        &self,
        payment: &Payment,
        _outs: &(),
        ctx: &mut Context<'_, (), AppState>,
    ) -> Result<(), HandlerOutcome> {
        if let Err(err) = ctx.state().gateway.transfer(payment).await {
            tracing::warn!(payment.id, %err, "refund failed, retrying");
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}

// --8<-- [start:dead_letters]
/// The dead-letter stream is read by an ordinary subscription. The retry-count header says how
/// many times the payment was delivered before it was carried here.
struct OnDeadLetter;

impl Handle<Payment, (), (), (), AppState> for OnDeadLetter {
    fn handle(
        &self,
        payment: &Payment,
        _outs: &(),
        ctx: &mut Context<'_, (), AppState>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        let deliveries = ctx.headers().get_str(RETRY_COUNT_HEADER).unwrap_or("1");
        tracing::error!(payment.id, deliveries, "payment dead-lettered");
        ready(Ok(()))
    }
}

fn dead_letters() -> RedisStream {
    RedisStream::new("payments.dlq").group("operators")
}
// --8<-- [end:dead_letters]

// --8<-- [start:declaration]
fn app() -> impl App<State = AppState> {
    RustStream::new(AppInfo::new("billing", "0.1.0"))
        .on_startup(async move |()| {
            let gateway = PaymentGateway {
                endpoint: "https://gateway.internal".into(),
            };
            Ok::<_, Infallible>(AppState { gateway })
        })
        .with_broker(RedisBroker::standalone("redis://localhost:6379"), |b| {
            // Five deliveries per payment, the first included. The fifth failure goes to the
            // `payments.dlq` stream with `XADD`, payload and headers as they arrived.
            let payments = RedisStream::new("payments").group("billing");
            b.include(subscriber(payments, Charge).build())
                .max_attempts(nonzero!(5u32))
                .dead_letter("payments.dlq");
            // Three deliveries per refund; the third failure is `LPUSH`ed onto `refunds.failed`.
            let refunds = RedisList::new("refunds").reliable();
            b.include(subscriber(refunds, Refund).build())
                .max_attempts(nonzero!(3u32))
                .dead_letter("refunds.failed");
            b.include(subscriber(dead_letters(), OnDeadLetter).build());
        })
}
// --8<-- [end:declaration]

fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::runtime::RETRY_COUNT_HEADER;
    use ruststream::testing::TestApp;
    use ruststream_fred::prelude::*;

    use super::{Payment, app};

    /// The gateway rejects every amount ending in 7.
    const REJECTED: Payment = Payment {
        id: 7,
        amount_cents: 1_007,
    };

    #[tokio::test]
    async fn the_fifth_failed_charge_is_dead_lettered() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<RedisBroker>()
            .message(&REJECTED)
            .to("payments")
            .publish()
            .await?;

        tb.broker::<RedisBroker>()
            .subscriber("payments")
            .assert_called(5);
        tb.broker::<RedisBroker>()
            .published::<Payment>("payments.dlq")
            .assert_called_once()
            .with(&REJECTED)
            .with_header(RETRY_COUNT_HEADER, "5");
        tb.broker::<RedisBroker>()
            .subscriber("payments.dlq")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        Ok(())
    }

    #[tokio::test]
    async fn the_third_failed_refund_lands_on_its_list() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<RedisBroker>()
            .message(&REJECTED)
            .to("refunds")
            .publish()
            .await?;

        tb.broker::<RedisBroker>()
            .subscriber("refunds")
            .assert_called(3);
        tb.broker::<RedisBroker>()
            .published::<Payment>("refunds.failed")
            .assert_called_once()
            .with(&REJECTED);
        Ok(())
    }
}
