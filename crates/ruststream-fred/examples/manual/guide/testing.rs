//! The payments service of the Testing a Redis service guide and its tests, written without the
//! macros: the handlers are types with an `impl Handle`, mounted with the `subscriber` constructor,
//! and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_guide_testing --features testing
//! REDIS_TEST_URL=redis://127.0.0.1:6379 cargo test --example manual_guide_testing --features testing
//! ```

use std::process::ExitCode;
use std::time::Duration;

use fred::interfaces::KeysInterface;
use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_fred::prelude::*;
use serde::{Deserialize, Serialize};

#[cfg(test)]
#[path = "../../../tests/live/mod.rs"]
mod live;

/// How much one account may spend in a day.
const DAILY_LIMIT_CENTS: i64 = 100_000;

/// How long a payment over the limit waits before it is tried again.
const HOLD: Duration = Duration::from_secs(60);

/// Declares the destination a type names, as `#[outgoing(name = "..")]` does.
macro_rules! named {
    ($ty:ty, $name:literal) => {
        impl OutgoingDestination for $ty {
            type Form = FixedName;
            const DESTINATION: &'static str = $name;
        }

        impl MessageHeaders for $ty {
            type Contract = NoHeaders;
        }
    };
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Payment {
    id: u64,
    account: String,
    amount_cents: i64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Receipt {
    payment_id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Refund {
    account: String,
    amount_cents: i64,
}

named!(Payment, "card-payments");
named!(Receipt, "card-receipts");
named!(Refund, "card-refunds");

fn spent_key(account: &str) -> String {
    format!("spent:{account}")
}

struct Charge;

impl Handle<Payment, Receipt, (), PoolContext> for Charge {
    async fn handle(
        &self,
        payment: &Payment,
        _outs: &(),
        ctx: &mut Context<'_, PoolContext>,
    ) -> Result<Receipt, HandlerOutcome> {
        if payment.amount_cents <= 0 {
            return Err(HandlerOutcome::drop());
        }
        let pool = ctx.context(keys::FredPool);
        let key = spent_key(&payment.account);
        let spent: i64 = pool
            .incr_by(&key, payment.amount_cents)
            .await
            .map_err(|_| HandlerOutcome::retry())?;
        if spent > DAILY_LIMIT_CENTS {
            // Give the room back: the payment is not charged yet.
            let _: i64 = pool
                .decr_by(&key, payment.amount_cents)
                .await
                .map_err(|_| HandlerOutcome::retry())?;
            tracing::warn!(payment.id, spent, "daily limit reached, payment held");
            return Err(HandlerOutcome::retry_after(HOLD));
        }
        Ok(Receipt {
            payment_id: payment.id,
        })
    }
}

struct GiveBack;

impl Handle<Refund, (), (), PoolContext> for GiveBack {
    async fn handle(
        &self,
        refund: &Refund,
        _outs: &(),
        ctx: &mut Context<'_, PoolContext>,
    ) -> Result<(), HandlerOutcome> {
        let key = spent_key(&refund.account);
        match ctx
            .context(keys::FredPool)
            .decr_by::<i64, _>(&key, refund.amount_cents)
            .await
        {
            Ok(_) => Ok(()),
            Err(_) => Err(HandlerOutcome::retry()),
        }
    }
}

/// The service on the server at `url`.
fn service(url: &str) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(
        RedisBroker::standalone(url),
        |b| {
            b.include(
                subscriber(RedisStream::new("card-payments").group("billing"), Charge)
                    .reply()
                    .build(),
            )
            .max_attempts(nonzero!(3u32))
            .dead_letter("card-payments.review");
            b.include(subscriber(RedisList::new("card-refunds").reliable(), GiveBack).build());
        },
    )
}

fn app() -> impl App<State = ()> {
    service("redis://localhost:6379")
}

fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::{SystemTime, UNIX_EPOCH};

    use ruststream::runtime::RETRY_COUNT_HEADER;
    use ruststream::testing::{Outcome, TestApp};
    use ruststream_fred::prelude::*;

    use super::{HOLD, Payment, Receipt, Refund, app, live, service};

    fn payment(id: u64, account: &str, amount_cents: i64) -> Payment {
        Payment {
            id,
            account: account.to_owned(),
            amount_cents,
        }
    }

    // --8<-- [start:receipt]
    /// The body both modes run; only the start call differs.
    async fn a_payment_is_answered_with_a_receipt(
        tb: TestApp<()>,
        account: &str,
    ) -> Result<(), Box<dyn Error>> {
        tb.broker::<RedisBroker>()
            .message(&payment(1, account, 2500))
            .publish()
            .await?;

        tb.broker::<RedisBroker>()
            .subscriber("card-payments")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.broker::<RedisBroker>()
            .published::<Receipt>("card-receipts")
            .assert_called_once()
            .with(&Receipt { payment_id: 1 });
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn in_process() -> Result<(), Box<dyn Error>> {
        a_payment_is_answered_with_a_receipt(TestApp::start(app()).await?, "acct-1").await
    }
    // --8<-- [end:receipt]

    // --8<-- [start:dropped]
    #[tokio::test]
    async fn a_payment_without_an_amount_is_dropped() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<RedisBroker>()
            .message(&payment(2, "acct-2", 0))
            .publish()
            .await?;

        tb.broker::<RedisBroker>()
            .subscriber("card-payments")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.broker::<RedisBroker>()
            .published::<Receipt>("card-receipts")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }
    // --8<-- [end:dropped]

    // --8<-- [start:held]
    #[tokio::test(start_paused = true)]
    async fn a_held_payment_passes_once_a_refund_frees_room() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let redis = tb.broker::<RedisBroker>();

        redis
            .message(&payment(3, "acct-3", 80_000))
            .publish()
            .await?;
        redis
            .message(&payment(4, "acct-3", 30_000))
            .publish()
            .await?;
        redis
            .subscriber("card-payments")
            .assert_called(2)
            .settled(HandlerOutcome::retry_after(HOLD));

        // The refund is a list entry; the handler gives the room back through the pool.
        redis
            .message(&Refund {
                account: "acct-3".to_owned(),
                amount_cents: 50_000,
            })
            .publish()
            .await?;
        tb.advance(HOLD).await?;

        assert_eq!(
            redis.subscriber("card-payments").outcomes(),
            [Outcome::Ack, Outcome::Nack, Outcome::Ack],
        );
        redis
            .published::<Receipt>("card-receipts")
            .assert_called(2)
            .with(&Receipt { payment_id: 4 });
        tb.shutdown().await?;
        Ok(())
    }
    // --8<-- [end:held]

    // --8<-- [start:review]
    #[tokio::test(start_paused = true)]
    async fn a_payment_held_three_times_goes_to_review() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<RedisBroker>()
            .message(&payment(5, "acct-5", 150_000))
            .publish()
            .await?;
        tb.advance(HOLD).await?;
        tb.advance(HOLD).await?;

        tb.broker::<RedisBroker>()
            .subscriber("card-payments")
            .assert_called(3);
        tb.broker::<RedisBroker>()
            .published::<Payment>("card-payments.review")
            .assert_called_once()
            .with(&payment(5, "acct-5", 150_000))
            .with_header(RETRY_COUNT_HEADER, "3");
        tb.shutdown().await?;
        Ok(())
    }
    // --8<-- [end:review]

    // --8<-- [start:live]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn against_a_server() -> Result<(), Box<dyn Error>> {
        let Some(url) = live::url("REDIS_TEST_URL") else {
            return Ok(());
        };
        // A server keeps what earlier runs left, so each run spends from an account of its own.
        let run = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let account = format!("acct-{run}");
        a_payment_is_answered_with_a_receipt(TestApp::start_live(service(&url)).await?, &account)
            .await
    }
    // --8<-- [end:live]
}
