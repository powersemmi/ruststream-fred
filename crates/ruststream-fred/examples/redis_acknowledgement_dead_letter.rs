//! Capping the retries at the mount site: a message that keeps failing goes to a dead-letter
//! destination, written with the command of the form it came from.
//!
//! ```text
//! cargo test --example redis_acknowledgement_dead_letter --features testing
//! cargo run --example redis_acknowledgement_dead_letter --features testing -- run
//! ```

use std::convert::Infallible;

use ruststream::runtime::RETRY_COUNT_HEADER;
use ruststream_fred::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Payment {
    id: u64,
    amount_cents: i64,
}

// A stand-in for a payment gateway client that rejects some payments every time: wired once at
// startup, injected into handlers through `State`.
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

// `#[derive(FromRef)]` makes each state field injectable with `State<FieldType>`.
#[derive(FromRef)]
struct AppState {
    gateway: PaymentGateway,
}

/// A failed charge asks for a retry; the handler counts nothing itself.
#[subscriber(RedisStream::new("payments").group("billing"))]
async fn charge(payment: &Payment, State(gateway): State<PaymentGateway>) -> HandlerOutcome {
    match gateway.transfer(payment).await {
        Ok(()) => HandlerOutcome::ack(),
        Err(err) => {
            tracing::warn!(payment.id, %err, "charge failed, retrying");
            HandlerOutcome::retry()
        }
    }
}

/// The same on a reliable list of refunds.
#[subscriber(RedisList::new("refunds").reliable())]
async fn refund(payment: &Payment, State(gateway): State<PaymentGateway>) -> HandlerOutcome {
    match gateway.transfer(payment).await {
        Ok(()) => HandlerOutcome::ack(),
        Err(err) => {
            tracing::warn!(payment.id, %err, "refund failed, retrying");
            HandlerOutcome::retry()
        }
    }
}

// --8<-- [start:dead_letters]
/// The dead-letter stream is read by an ordinary subscription. The retry-count header says how
/// many times the payment was delivered before it was carried here.
#[subscriber(RedisStream::new("payments.dlq").group("operators"))]
async fn on_dead_letter(payment: &Payment, ctx: &mut Context<'_>) -> HandlerOutcome {
    let deliveries = ctx.headers().get_str(RETRY_COUNT_HEADER).unwrap_or("1");
    tracing::error!(payment.id, deliveries, "payment dead-lettered");
    HandlerOutcome::ack()
}
// --8<-- [end:dead_letters]

// --8<-- [start:declaration]
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
            // Five deliveries per payment, the first included. The fifth failure goes to the
            // `payments.dlq` stream with `XADD`, payload and headers as they arrived.
            b.include(charge)
                .max_attempts(nonzero!(5u32))
                .dead_letter("payments.dlq");
            // Three deliveries per refund; the third failure is `LPUSH`ed onto `refunds.failed`.
            b.include(refund)
                .max_attempts(nonzero!(3u32))
                .dead_letter("refunds.failed");
            b.include(on_dead_letter);
        })
}
// --8<-- [end:declaration]

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
