//! `RedisError` where a service meets it: a write the server refuses, and a startup that fails.
//!
//! ```text
//! cargo test --example redis_broker_errors --features testing
//! ```

use std::time::Duration;

use serde::{Deserialize, Serialize};

use ruststream_fred::prelude::*;

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
    amount: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "ledger")]
struct LedgerEntry {
    order_id: u64,
    amount: u64,
}

#[derive(OutSlot)]
#[publishes(LedgerEntry)]
struct Ledger;

// --8<-- [start:handler]
/// A retried order stays pending and comes back once it has been idle for thirty seconds.
#[subscriber(RedisStream::claiming("orders", Duration::from_secs(30)).group("workers"))]
async fn book(order: &Order, Out(ledger): Out<impl Publisher, Ledger>) -> HandlerOutcome {
    let entry = LedgerEntry {
        order_id: order.id,
        amount: order.amount,
    };
    if let Err(err) = ledger.message(&entry).publish().await {
        // The error carries the server's answer, such as `WRONGTYPE`.
        tracing::warn!(subscription = "orders", order.id, error = %err, "ledger entry not written");
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

#[ruststream::app]
fn app() -> impl App<State = ()> {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            b.include(book).out(Ledger, list::Publish::new()).build();
        },
    )
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::{LedgerEntry, Order, app};
    use ruststream::testing::TestApp;
    use ruststream_fred::RedisError;
    use ruststream_fred::prelude::*;

    #[tokio::test]
    async fn a_written_entry_acks_the_order() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<RedisBroker>()
            .message(&Order { id: 1, amount: 40 })
            .publish()
            .await?;

        tb.broker::<RedisBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.broker::<RedisBroker>()
            .published::<LedgerEntry>("ledger")
            .assert_called_once()
            .with(&LedgerEntry {
                order_id: 1,
                amount: 40,
            });
        Ok(())
    }

    #[tokio::test]
    async fn a_refused_write_retries_the_order() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        // An entry added to `ledger` makes it a stream, so a list push to it is refused with
        // `WRONGTYPE`.
        let old = LedgerEntry {
            order_id: 0,
            amount: 0,
        };
        tb.broker::<RedisBroker>().message(&old).publish().await?;
        tb.broker::<RedisBroker>()
            .message(&Order { id: 2, amount: 10 })
            .publish()
            .await?;

        tb.broker::<RedisBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::retry());
        Ok(())
    }

    #[subscriber("orders")]
    async fn by_name(order: &Order) -> HandlerOutcome {
        let _ = order.id;
        HandlerOutcome::ack()
    }

    #[tokio::test]
    async fn a_failed_startup_carries_the_redis_error() {
        // A bare name and no default group: a stream is read only through a group.
        let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
            RedisBroker::standalone("redis://localhost:6379"),
            |b| {
                b.include(by_name);
            },
        );

        let failed = TestApp::start(app).await.err();
        let mut cause: Option<&(dyn Error + 'static)> = failed.as_ref().map(|err| err as _);
        while let Some(err) = cause {
            if let Some(redis) = err.downcast_ref::<RedisError>() {
                assert!(matches!(redis, RedisError::InvalidOptions(_)), "{redis}");
                return;
            }
            cause = err.source();
        }
        panic!("startup did not fail with a RedisError: {failed:?}");
    }
}
