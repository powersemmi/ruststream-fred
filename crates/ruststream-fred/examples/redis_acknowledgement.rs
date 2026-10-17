//! What a handler's outcome does on a Redis stream: an ack is `XACK`, a drop acknowledges the
//! entry without a copy, and a retry appends a copy to the stream before it acknowledges.
//!
//! ```text
//! cargo test --example redis_acknowledgement --features testing
//! cargo run --example redis_acknowledgement --features testing -- run
//! ```

use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use ruststream_fred::stream::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "payments")]
struct Payment {
    id: u64,
    amount_cents: i64,
}

// A stand-in for a payment gateway client: it is busy on the first charge it is asked for and
// takes every charge after that. Wired once at startup, injected through `State`.
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

// `#[derive(FromRef)]` makes each state field injectable with `State<FieldType>`.
#[derive(FromRef)]
struct AppState {
    gateway: PaymentGateway,
}

// --8<-- [start:outcomes]
/// One handler, three outcomes: the ack settles the entry, the drop settles it without a copy,
/// the retry appends a copy to `payments` and settles the original.
#[subscriber(RedisStream::new("payments").group("billing"))]
async fn charge(payment: &Payment, State(gateway): State<PaymentGateway>) -> HandlerOutcome {
    if payment.amount_cents <= 0 {
        tracing::warn!(payment.id, "malformed payment dropped");
        return HandlerOutcome::drop();
    }
    match gateway.charge(payment).await {
        Ok(()) => HandlerOutcome::ack(),
        Err(err) => {
            tracing::warn!(payment.id, %err, "charge failed, retrying");
            HandlerOutcome::retry()
        }
    }
}
// --8<-- [end:outcomes]

#[ruststream::app]
fn app() -> impl App<State = AppState> {
    RustStream::new(AppInfo::new("billing", "0.1.0"))
        .on_startup(async move |()| {
            Ok::<_, Infallible>(AppState {
                gateway: PaymentGateway::default(),
            })
        })
        .with_broker(RedisBroker::standalone("redis://localhost:6379"), |b| {
            b.include(charge);
        })
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
