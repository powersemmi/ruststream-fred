//! An atomic window: each delivery's commands and its `XACK` leave as one `MULTI` / `EXEC`, so the
//! handler's Redis side effects and the acknowledgement happen together or not at all.
//!
//! ```text
//! cargo test --example redis_pipelining_atomic --features testing
//! cargo run --example redis_pipelining_atomic -- run
//! ```

use ruststream_fred::stream::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
    paid: bool,
}

// --8<-- [start:atomic]
/// Queues an invoice beside the order; the hash tag `{orders}` keeps both keys on one cluster slot.
#[subscriber(RedisStream::new("{orders}").group("billing"))]
async fn invoice(order: &Order, Ctx(pipeline): Ctx<keys::Pipeline>) -> HandlerOutcome {
    if pipeline
        .lpush("{orders}:invoices", order.id.to_string())
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    if !order.paid {
        tracing::warn!(order.id, "an unpaid order gets no invoice");
        return HandlerOutcome::drop();
    }
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            b.include(invoice.pipeline().atomic());
        },
    )
}
// --8<-- [end:atomic]

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
