//! The service of `examples/redis_broker_errors.rs` without the macros: the handler is a named
//! type, the slot a marker type, and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_redis_broker_errors --features testing
//! ```

use std::process::ExitCode;
use std::time::Duration;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream_fred::prelude::*;

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
    amount: u64,
}

// `#[outgoing(name = "orders")]` by hand: an order names the stream it is added to.
impl OutgoingDestination for Order {
    type Form = FixedName;
    const DESTINATION: &'static str = "orders";
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct LedgerEntry {
    order_id: u64,
    amount: u64,
}

// `#[outgoing(name = "ledger")]` by hand: an entry names the list it is pushed to.
impl OutgoingDestination for LedgerEntry {
    type Form = FixedName;
    const DESTINATION: &'static str = "ledger";
}

impl MessageHeaders for LedgerEntry {
    type Contract = NoHeaders;
}

/// The slot ledger entries leave through.
struct Ledger;

impl OutSlot for Ledger {
    const NAME: &'static str = "Ledger";
    // `LedgerEntry` fixes its destination, so no transform on this slot may name one.
    type Destination = Reads;
}

impl PublishedThrough<Ledger> for LedgerEntry {}

// --8<-- [start:handler]
struct Book;

impl<Entry> Handle<Order, (), Outs<(Entry,)>> for Book
where
    Entry: OutEntry<Ledger, Wire: Publisher>,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(Entry,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let entry = LedgerEntry {
            order_id: order.id,
            amount: order.amount,
        };
        if let Err(err) = outs.get(Ledger).message(&entry).publish().await {
            // The error carries the server's answer, such as `WRONGTYPE`.
            tracing::warn!(subscription = "orders", order.id, error = %err, "ledger entry not written");
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}

/// A retried order stays pending and comes back once it has been idle for thirty seconds.
fn orders() -> RedisStream {
    RedisStream::claiming("orders", Duration::from_secs(30)).group("workers")
}
// --8<-- [end:handler]

fn app() -> impl App<State = ()> {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            b.include(subscriber(orders(), Book).build())
                .out(Ledger, list::Publish::new())
                .build();
        },
    )
}

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::{Book, Ledger, LedgerEntry, Order, app};
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

    #[tokio::test]
    async fn a_failed_startup_carries_the_redis_error() {
        // A bare name and no default group: a stream is read only through a group.
        let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
            RedisBroker::standalone("redis://localhost:6379"),
            |b| {
                b.include(subscriber("orders", Book).build())
                    .out(Ledger, list::Publish::new())
                    .build();
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
