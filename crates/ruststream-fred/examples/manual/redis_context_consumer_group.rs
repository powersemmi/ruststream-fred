//! The service of `examples/redis_context_consumer_group.rs` written without `#[subscriber]`: a
//! body typed over `StreamContext` reads the group off its context.
//!
//! ```text
//! cargo test --example manual_redis_context_consumer_group --features testing
//! cargo run --example manual_redis_context_consumer_group -- run
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

// --8<-- [start:consumer_group]
struct Bill;

impl Handle<Order, Invoice, (), StreamContext> for Bill {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        ctx: &mut Context<'_, StreamContext>,
    ) -> impl Future<Output = Result<Invoice, HandlerOutcome>> {
        let group = ctx.context(keys::ConsumerGroup);
        tracing::info!(order.id, group, "order billed");
        ready(Ok(Invoice {
            order_id: order.id,
            billed_by: group.to_owned(),
        }))
    }
}

fn app() -> impl App {
    let broker = RedisBroker::standalone("redis://localhost:6379").default_group("billing");
    RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(broker, |b| {
        b.include(subscriber("orders", Bill).reply().build())
            .out_reply(Publish);
    })
}
// --8<-- [end:consumer_group]

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
    async fn an_invoice_names_the_default_group() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<RedisBroker>()
            .message(&Order { id: 4 })
            .publish()
            .await?;

        tb.broker::<RedisBroker>()
            .published::<Invoice>("invoices")
            .assert_called_once()
            .with(&Invoice {
                order_id: 4,
                billed_by: "billing".to_owned(),
            });
        tb.shutdown().await?;
        Ok(())
    }
}
