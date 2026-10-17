//! The service of `examples/redis_positions_entry_id.rs` written without the macros: a body on
//! the stream context reads the `EntryId` key and returns a reply.
//!
//! ```text
//! cargo test --example manual_redis_positions_entry_id --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::future::{Future, ready};

use ruststream::schemars::{self, JsonSchema};
use ruststream_fred::stream::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
struct Order {
    id: u64,
}

// `#[outgoing(name = "orders")]` by hand: the type fixes its destination.
impl OutgoingDestination for Order {
    type Form = FixedName;

    const DESTINATION: &'static str = "orders";
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

/// What billing returns for an order: the entry it billed, and when the server added it.
#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
struct Receipt {
    order: u64,
    entry: String,
    added_ms: u64,
}

// `#[outgoing(name = "receipts")]` by hand.
impl OutgoingDestination for Receipt {
    type Form = FixedName;

    const DESTINATION: &'static str = "receipts";
}

impl MessageHeaders for Receipt {
    type Contract = NoHeaders;
}

impl MessageInfo for Receipt {
    const NAME: &'static str = "Receipt";
}

// --8<-- [start:entry-id]
/// The receipt names the entry it bills, so a duplicate bill is traced to one stream entry.
struct Bill;

impl Handle<Order, Receipt, (), StreamContext> for Bill {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        ctx: &mut Context<'_, StreamContext>,
    ) -> impl Future<Output = Result<Receipt, HandlerOutcome>> {
        let entry = ctx.context(keys::EntryId);
        tracing::info!(order.id, %entry, "order billed");
        ready(Ok(Receipt {
            order: order.id,
            entry: entry.to_string(),
            // The first half of the id is the server clock at `XADD`.
            added_ms: entry.milliseconds(),
        }))
    }
}

fn app() -> impl App {
    let broker = RedisBroker::standalone("redis://localhost:6379");
    RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(broker, |b| {
        let orders = RedisStream::new("orders").group("billing");
        b.include(subscriber(orders, Bill).reply().build());
    })
}
// --8<-- [end:entry-id]

// The scenarios are the tests below; the command in the header runs them.
fn main() {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_fred::EntryId;

    use super::*;

    #[tokio::test]
    async fn each_receipt_names_the_entry_it_billed() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<RedisBroker>();

        broker.message(&Order { id: 1 }).publish().await?;
        broker.message(&Order { id: 2 }).publish().await?;

        let receipts = broker.published::<Receipt>("receipts").decoded();
        let entries: Vec<EntryId> = receipts
            .iter()
            .map(|receipt| receipt.entry.parse())
            .collect::<Result<_, _>>()?;
        assert_eq!(entries.len(), 2);
        assert!(
            entries[0] < entries[1],
            "ids grow with the stream: {entries:?}"
        );
        for (receipt, entry) in receipts.iter().zip(&entries) {
            assert_eq!(receipt.added_ms, entry.milliseconds());
        }
        tb.shutdown().await?;
        Ok(())
    }
}
