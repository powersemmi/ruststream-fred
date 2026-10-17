//! The service of the Consumer groups and crash recovery guide: payments on one Redis stream, posted
//! to the ledger by one consumer group and screened for fraud by another.
//!
//! ```text
//! docker run -p 6379:6379 redis:8.4
//! cargo run --example guide_consumer_groups --features testing -- run
//! ```

// `main` runs the service for Redis 8.4; the service for older servers is built by its tests.
#![cfg_attr(not(test), allow(dead_code))]

use std::time::Duration;

use ruststream_fred::DELIVERY_COUNT_HEADER;
use ruststream_fred::stream::prelude::*;
use serde::{Deserialize, Serialize};

// --8<-- [start:ledger]
/// Longer than the slowest posting ever takes: an entry idle this long belongs to no live worker.
const MIN_IDLE: Duration = Duration::from_secs(30);

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "payments")]
struct Payment {
    id: u64,
    account: String,
    amount_cents: u64,
}

#[subscriber(
    RedisStream::claiming("payments", MIN_IDLE).group("ledger"),
    workers(8, by_key)
)]
async fn post_payment(payment: &Payment, ctx: &mut Context<'_>) -> HandlerOutcome {
    let earlier_deliveries = ctx.headers().get_str(DELIVERY_COUNT_HEADER).unwrap_or("0");
    if payment.account.starts_with("migrating-") {
        tracing::warn!(
            payment.id,
            earlier_deliveries,
            "account locked, payment left pending"
        );
        return HandlerOutcome::retry();
    }
    tracing::info!(payment.id, earlier_deliveries, "payment posted");
    HandlerOutcome::ack()
}
// --8<-- [end:ledger]

// --8<-- [start:fraud]
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "payments.screened")]
struct Screened {
    payment_id: u64,
    suspicious: bool,
}

#[subscriber(
    RedisStream::new("payments")
        .group("fraud")
        .start_id(StreamStart::Beginning),
    reply
)]
async fn screen_payment(payment: &Payment) -> Screened {
    Screened {
        payment_id: payment.id,
        suspicious: payment.amount_cents > 1_000_000,
    }
}
// --8<-- [end:fraud]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            b.include(post_payment);
            b.include(screen_payment).out_reply(Publish);
        },
    )
}
// --8<-- [end:app]

// --8<-- [start:legacy]
#[subscriber(RedisStream::new("payments").group("ledger"))]
async fn post_new_payment(payment: &Payment) -> HandlerOutcome {
    tracing::info!(payment.id, "payment posted");
    HandlerOutcome::ack()
}

#[subscriber(RedisStream::reclaim("payments", MIN_IDLE).group("ledger"))]
async fn recover_payment(payment: &Payment, ctx: &mut Context<'_>) -> HandlerOutcome {
    let deliveries = ctx.headers().get_str(DELIVERY_COUNT_HEADER).unwrap_or("1");
    tracing::warn!(
        payment.id,
        deliveries,
        "payment recovered from a stopped worker"
    );
    HandlerOutcome::ack()
}

fn app_before_8_4() -> impl App {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            b.include(post_new_payment);
            b.include(recover_payment);
            b.include(screen_payment).out_reply(Publish);
        },
    )
}
// --8<-- [end:legacy]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_fred::stream::prelude::*;

    use super::{MIN_IDLE, Payment, Screened, app, app_before_8_4};

    fn payment(id: u64, account: &str) -> Payment {
        Payment {
            id,
            account: account.to_owned(),
            amount_cents: 2500,
        }
    }

    #[tokio::test]
    async fn each_group_reads_every_payment() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<RedisBroker>()
            .message(&payment(1, "acct-1"))
            .publish()
            .await?;

        // One delivery to the ledger group and one to the fraud group.
        tb.broker::<RedisBroker>()
            .subscriber("payments")
            .assert_called(2)
            .settled(HandlerOutcome::ack());
        tb.broker::<RedisBroker>()
            .published::<Screened>("payments.screened")
            .assert_called_once()
            .with(&Screened {
                payment_id: 1,
                suspicious: false,
            });
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_pending_payment_comes_back_after_min_idle() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<RedisBroker>()
            .message(&payment(2, "migrating-7"))
            .publish()
            .await?;
        // The ledger group left the entry pending; the fraud group screened it.
        tb.broker::<RedisBroker>()
            .subscriber("payments")
            .assert_called(2);

        // Half the idle time: the entry may still belong to a live worker.
        tb.advance(MIN_IDLE / 2).await?;
        tb.broker::<RedisBroker>()
            .subscriber("payments")
            .assert_called(2);

        // The other half: the ledger group claims the entry back, under the same id.
        tb.advance(MIN_IDLE / 2).await?;
        tb.broker::<RedisBroker>()
            .subscriber("payments")
            .assert_called(3)
            .settled(HandlerOutcome::retry());
        tb.broker::<RedisBroker>()
            .published::<Payment>("payments")
            .assert_called_once();
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn before_8_4_an_acknowledged_payment_is_not_recovered() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app_before_8_4()).await?;

        tb.broker::<RedisBroker>()
            .message(&payment(3, "acct-3"))
            .publish()
            .await?;
        // The fresh-tail reader of the ledger group and the fraud group, not the reclaim reader.
        tb.broker::<RedisBroker>()
            .subscriber("payments")
            .assert_called(2)
            .settled(HandlerOutcome::ack());

        // Nothing is left pending, so the reclaim reader finds nothing to take.
        tb.advance(MIN_IDLE * 2).await?;
        tb.broker::<RedisBroker>()
            .subscriber("payments")
            .assert_called(2);
        tb.shutdown().await?;
        Ok(())
    }
}
