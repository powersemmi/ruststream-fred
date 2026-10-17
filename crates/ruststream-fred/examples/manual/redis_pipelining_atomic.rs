//! The service of `examples/redis_pipelining_atomic.rs` written without `#[subscriber]`: the
//! window steps chain on the manual subscriber the same way.
//!
//! ```text
//! cargo test --example manual_redis_pipelining_atomic --features testing
//! cargo run --example manual_redis_pipelining_atomic -- run
//! ```

use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_fred::stream::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
    paid: bool,
}

// --8<-- [start:atomic]
/// Queues an invoice beside the order; the hash tag `{orders}` keeps both keys on one cluster slot.
struct Invoice;

impl Handle<Order, (), (), PipelineContext> for Invoice {
    async fn handle(
        &self,
        order: &Order,
        _outs: &(),
        ctx: &mut Context<'_, PipelineContext>,
    ) -> Result<(), HandlerOutcome> {
        let pipeline = ctx.context(keys::Pipeline);
        if pipeline
            .lpush("{orders}:invoices", order.id.to_string())
            .await
            .is_err()
        {
            return Err(HandlerOutcome::retry());
        }
        if !order.paid {
            tracing::warn!(order.id, "an unpaid order gets no invoice");
            return Err(HandlerOutcome::drop());
        }
        Ok(())
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            let invoice = subscriber(RedisStream::new("{orders}").group("billing"), Invoice);
            b.include(invoice.pipeline().atomic().build());
        },
    )
}
// --8<-- [end:atomic]

fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_fred::stream::prelude::*;

    use super::{Order, app};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_paid_order_commits_its_invoice_with_the_ack() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<RedisBroker>()
            .message(&Order { id: 3, paid: true })
            .to("{orders}")
            .publish()
            .await?;

        tb.broker::<RedisBroker>()
            .subscriber("{orders}")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.broker::<RedisBroker>()
            .published::<u64>("{orders}:invoices")
            .assert_called_once()
            .with(&3);
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_unpaid_order_commits_nothing() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<RedisBroker>()
            .message(&Order { id: 4, paid: false })
            .to("{orders}")
            .publish()
            .await?;

        tb.broker::<RedisBroker>()
            .subscriber("{orders}")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.broker::<RedisBroker>()
            .published::<u64>("{orders}:invoices")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }
}
