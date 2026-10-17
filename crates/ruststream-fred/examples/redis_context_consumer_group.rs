//! `Ctx<keys::ConsumerGroup>` binds the consumer group a stream subscription reads through. Here
//! the group comes from the broker's default, so the handler learns it at run time.
//!
//! ```text
//! cargo test --example redis_context_consumer_group --features testing
//! cargo run --example redis_context_consumer_group -- run
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

// --8<-- [start:consumer_group]
#[subscriber("orders", reply)]
async fn bill(order: &Order, Ctx(group): Ctx<keys::ConsumerGroup>) -> Invoice {
    tracing::info!(order.id, %group, "order billed");
    Invoice {
        order_id: order.id,
        billed_by: group,
    }
}

#[ruststream::app]
fn app() -> impl App {
    let broker = RedisBroker::standalone("redis://localhost:6379").default_group("billing");
    RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(broker, |b| {
        b.include(bill).out_reply(Publish);
    })
}
// --8<-- [end:consumer_group]

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
