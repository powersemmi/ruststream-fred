//! The service of `examples/redis_context_batch.rs` written without `#[subscriber]`: a batch body
//! typed over `StreamBatchContext` reads the group once per batch.
//!
//! ```text
//! cargo test --example manual_redis_context_batch --features testing
//! cargo run --example manual_redis_context_batch -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_fred::stream::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

/// An invoice, stamped with the group that billed it.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "invoices")]
struct Invoice {
    order_id: u64,
    billed_by: String,
}

// --8<-- [start:batch]
/// Bills a batch of orders; the group is read once, off the batch's context.
struct BillAll;

impl Handle<[Order], Vec<Invoice>, (), StreamBatchContext> for BillAll {
    fn handle(
        &self,
        orders: &[Order],
        _outs: &(),
        ctx: &mut Context<'_, StreamBatchContext>,
    ) -> impl Future<Output = Result<Vec<Invoice>, Vec<HandlerOutcome>>> {
        let group = ctx.context(keys::ConsumerGroup);
        tracing::info!(orders = orders.len(), group, "batch billed");
        ready(Ok(orders
            .iter()
            .map(|order| Invoice {
                order_id: order.id,
                billed_by: group.to_owned(),
            })
            .collect()))
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            let bill_all = subscriber(RedisStream::new("orders").group("billing"), BillAll);
            b.include(bill_all.batch(nonzero!(32)).reply().build())
                .out_reply(Publish);
        },
    )
}
// --8<-- [end:batch]

fn main() -> ExitCode {
    run_main(app)
}

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
