//! A stream batch handler's context, `StreamBatchContext`, carries what belongs to the whole
//! subscription: the consumer group and the group's reposition handle.
//!
//! ```text
//! cargo test --example redis_context_batch --features testing
//! cargo run --example redis_context_batch -- run
//! ```

use ruststream_fred::stream::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

/// An invoice, stamped with the group that billed it.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "invoices")]
struct Invoice {
    order_id: u64,
    billed_by: String,
}

// --8<-- [start:batch]
/// Bills a batch of orders; the group is read once, off the batch's context.
#[subscriber(RedisStream::new("orders").group("billing"), reply)]
async fn bill_all(
    orders: &[Order],
    ctx: &mut Context<'_, StreamBatchContext>,
) -> Result<Vec<Invoice>, HandlerOutcome> {
    let group = ctx.context(keys::ConsumerGroup);
    tracing::info!(orders = orders.len(), group, "batch billed");
    Ok(orders
        .iter()
        .map(|order| Invoice {
            order_id: order.id,
            billed_by: group.to_owned(),
        })
        .collect())
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            b.include(bill_all.batch(nonzero!(32))).out_reply(Publish);
        },
    )
}
// --8<-- [end:batch]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_fred::stream::prelude::*;

    use super::{Invoice, Order, app};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn every_invoice_of_a_batch_names_the_group() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        for id in [1, 2] {
            tb.broker::<RedisBroker>()
                .message(&Order { id })
                .publish()
                .await?;
        }

        let invoices = tb
            .broker::<RedisBroker>()
            .published::<Invoice>("invoices")
            .assert_called(2)
            .decoded();
        let billed: Vec<(u64, &str)> = invoices
            .iter()
            .map(|invoice| (invoice.order_id, invoice.billed_by.as_str()))
            .collect();
        assert_eq!(billed, [(1, "billing"), (2, "billing")]);
        tb.shutdown().await?;
        Ok(())
    }
}
