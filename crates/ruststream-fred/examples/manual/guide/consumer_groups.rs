//! The service of the Consumer groups and crash recovery guide, written without the macros: the
//! handlers are types with an `impl Handle`, mounted with the `subscriber` constructor, and `main`
//! is written by hand.
//!
//! ```text
//! docker run -p 6379:6379 redis:8.4
//! cargo run --example manual_guide_consumer_groups --features testing -- run
//! ```

// `main` runs the service for Redis 8.4; the service for older servers is built by its tests.
#![cfg_attr(not(test), allow(dead_code))]

use std::future::{Future, ready};
use std::process::ExitCode;
use std::time::Duration;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_fred::DELIVERY_COUNT_HEADER;
use ruststream_fred::stream::prelude::*;
use serde::{Deserialize, Serialize};

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

// --8<-- [start:ledger]
/// Longer than the slowest posting ever takes: an entry idle this long belongs to no live worker.
const MIN_IDLE: Duration = Duration::from_secs(30);

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Payment {
    id: u64,
    account: String,
    amount_cents: u64,
}

named!(Payment, "payments");

struct PostPayment;

impl Handle<Payment> for PostPayment {
    fn handle(
        &self,
        payment: &Payment,
        _outs: &(),
        ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        let earlier_deliveries = ctx.headers().get_str(DELIVERY_COUNT_HEADER).unwrap_or("0");
        if payment.account.starts_with("migrating-") {
            tracing::warn!(
                payment.id,
                earlier_deliveries,
                "account locked, payment left pending"
            );
            return ready(Err(HandlerOutcome::retry()));
        }
        tracing::info!(payment.id, earlier_deliveries, "payment posted");
        ready(Ok(()))
    }
}
// --8<-- [end:ledger]

// --8<-- [start:fraud]
#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Screened {
    payment_id: u64,
    suspicious: bool,
}

named!(Screened, "payments.screened");

struct ScreenPayment;

impl Handle<Payment, Screened> for ScreenPayment {
    fn handle(
        &self,
        payment: &Payment,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Screened, HandlerOutcome>> {
        ready(Ok(Screened {
            payment_id: payment.id,
            suspicious: payment.amount_cents > 1_000_000,
        }))
    }
}
// --8<-- [end:fraud]

// --8<-- [start:app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            b.include(
                subscriber(
                    RedisStream::claiming("payments", MIN_IDLE).group("ledger"),
                    PostPayment,
                )
                .workers_by_key(nonzero!(8))
                .build(),
            );
            b.include(
                subscriber(
                    RedisStream::new("payments")
                        .group("fraud")
                        .start_id(StreamStart::Beginning),
                    ScreenPayment,
                )
                .reply()
                .build(),
            )
            .out_reply(Publish);
        },
    )
}

fn main() -> ExitCode {
    run_main(app)
}
// --8<-- [end:app]

// --8<-- [start:legacy]
struct PostNewPayment;

impl Handle<Payment> for PostNewPayment {
    fn handle(
        &self,
        payment: &Payment,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(payment.id, "payment posted");
        ready(Ok(()))
    }
}

struct RecoverPayment;

impl Handle<Payment> for RecoverPayment {
    fn handle(
        &self,
        payment: &Payment,
        _outs: &(),
        ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        let deliveries = ctx.headers().get_str(DELIVERY_COUNT_HEADER).unwrap_or("1");
        tracing::warn!(
            payment.id,
            deliveries,
            "payment recovered from a stopped worker"
        );
        ready(Ok(()))
    }
}

fn app_before_8_4() -> impl App {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            b.include(
                subscriber(RedisStream::new("payments").group("ledger"), PostNewPayment).build(),
            );
            b.include(
                subscriber(
                    RedisStream::reclaim("payments", MIN_IDLE).group("ledger"),
                    RecoverPayment,
                )
                .build(),
            );
            b.include(
                subscriber(
                    RedisStream::new("payments")
                        .group("fraud")
                        .start_id(StreamStart::Beginning),
                    ScreenPayment,
                )
                .reply()
                .build(),
            )
            .out_reply(Publish);
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
