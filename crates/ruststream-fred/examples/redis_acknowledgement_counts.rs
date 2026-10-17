//! The delivery count and idle time Redis reports on a claimed stream entry, and the cap that
//! reads the same count.
//!
//! ```text
//! cargo test --example redis_acknowledgement_counts --features testing
//! cargo run --example redis_acknowledgement_counts --features testing -- run
//! ```

use std::convert::Infallible;
use std::time::Duration;

use ruststream_fred::stream::prelude::*;
use ruststream_fred::{DELIVERY_COUNT_HEADER, IDLE_MS_HEADER};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "payouts")]
struct Payout {
    id: u64,
    amount_cents: i64,
}

// A stand-in for a payment gateway client that times out on some payouts: wired once at startup,
// injected into handlers through `State`.
#[derive(Clone)]
struct PaymentGateway {
    endpoint: String,
}

impl PaymentGateway {
    async fn pay(&self, payout: &Payout) -> Result<(), String> {
        tokio::task::yield_now().await;
        if payout.amount_cents % 10 == 7 {
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

/// How long an unfinished payout stays pending before the subscription claims it back.
const MIN_IDLE: Duration = Duration::from_secs(30);

// --8<-- [start:counts]
/// A claimed entry carries how often Redis has handed it out before and how long it sat pending;
/// the handler logs both with every attempt.
#[subscriber(RedisStream::claiming("payouts", MIN_IDLE).group("payouts"))]
async fn pay(
    payout: &Payout,
    ctx: &mut Context<'_>,
    State(gateway): State<PaymentGateway>,
) -> HandlerOutcome {
    let earlier = ctx.headers().get_str(DELIVERY_COUNT_HEADER).unwrap_or("0");
    let idle_ms = ctx.headers().get_str(IDLE_MS_HEADER).unwrap_or("0");
    tracing::info!(payout.id, earlier, idle_ms, "paying out");
    match gateway.pay(payout).await {
        Ok(()) => HandlerOutcome::ack(),
        Err(err) => {
            tracing::warn!(payout.id, %err, "payout failed, left pending");
            HandlerOutcome::retry()
        }
    }
}
// --8<-- [end:counts]

// --8<-- [start:declaration]
#[ruststream::app]
fn app() -> impl App<State = AppState> {
    RustStream::new(AppInfo::new("payouts", "0.1.0"))
        .on_startup(async move |()| {
            let gateway = PaymentGateway {
                endpoint: "https://gateway.internal".into(),
            };
            Ok::<_, Infallible>(AppState { gateway })
        })
        .with_broker(RedisBroker::standalone("redis://localhost:6379"), |b| {
            // The cap reads Redis's own count, so an entry a crashed worker never settled counts
            // too. The third failed delivery goes to `payouts.dlq`.
            b.include(pay)
                .max_attempts(nonzero!(3u32))
                .dead_letter("payouts.dlq");
        })
}
// --8<-- [end:declaration]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_fred::DELIVERY_COUNT_HEADER;
    use ruststream_fred::stream::prelude::*;

    use super::{MIN_IDLE, Payout, app};

    /// The gateway times out on every amount ending in 7.
    const TIMES_OUT: Payout = Payout {
        id: 7,
        amount_cents: 1_007,
    };

    #[tokio::test(start_paused = true)]
    async fn the_cap_counts_the_claims_redis_reports() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<RedisBroker>()
            .message(&TIMES_OUT)
            .publish()
            .await?;
        tb.broker::<RedisBroker>()
            .subscriber("payouts")
            .assert_called_once()
            .settled(HandlerOutcome::retry());

        // The entry stays pending in Redis; each wait of `MIN_IDLE` hands it out again.
        tb.advance(MIN_IDLE).await?;
        tb.advance(MIN_IDLE).await?;

        tb.broker::<RedisBroker>()
            .subscriber("payouts")
            .assert_called(3);
        tb.broker::<RedisBroker>()
            .published::<Payout>("payouts.dlq")
            .assert_called_once()
            .with(&TIMES_OUT)
            .with_header(DELIVERY_COUNT_HEADER, "2");
        Ok(())
    }
}
