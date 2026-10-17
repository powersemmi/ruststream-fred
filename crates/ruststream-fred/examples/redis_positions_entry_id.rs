//! A stream entry's id: the `EntryId` key binds the `<milliseconds>-<sequence>` id a delivery
//! was read at. Each scenario is a test.
//!
//! ```text
//! cargo test --example redis_positions_entry_id --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream_fred::stream::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

/// What billing returns for an order: the entry it billed, and when the server added it.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "receipts")]
struct Receipt {
    order: u64,
    entry: String,
    added_ms: u64,
}

// --8<-- [start:entry-id]
/// The receipt names the entry it bills, so a duplicate bill is traced to one stream entry.
#[subscriber(RedisStream::new("orders").group("billing"), reply)]
async fn bill(order: &Order, Ctx(entry): Ctx<keys::EntryId>) -> Receipt {
    tracing::info!(order.id, %entry, "order billed");
    Receipt {
        order: order.id,
        entry: entry.to_string(),
        // The first half of the id is the server clock at `XADD`.
        added_ms: entry.milliseconds(),
    }
}
// --8<-- [end:entry-id]

fn app() -> impl App {
    let broker = RedisBroker::standalone("redis://localhost:6379");
    RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(broker, |b| {
        b.include(bill);
    })
}

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
