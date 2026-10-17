//! `RedisPublish`, `Publish` in the stream prelude: the policy that appends each message to a
//! stream with `XADD`, here for a handler's reply.
//!
//! ```text
//! cargo test --example redis_publishing --features testing
//! cargo run --example redis_publishing -- run
//! ```

use ruststream_fred::stream::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

// --8<-- [start:publish]
/// The stream a receipt goes to is a property of the type.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "receipts")]
struct Receipt {
    order_id: u64,
}

#[subscriber(RedisStream::new("orders").group("workers"), reply)]
async fn confirm(order: &Order) -> Receipt {
    tracing::info!(order.id, "order confirmed");
    Receipt { order_id: order.id }
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            // Each receipt is an `XADD` to the `receipts` stream.
            b.include(confirm).out_reply(Publish);
        },
    )
}
// --8<-- [end:publish]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_fred::stream::prelude::*;

    use super::{Order, Receipt, app};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_confirmed_order_appends_its_receipt() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<RedisBroker>()
            .message(&Order { id: 12 })
            .publish()
            .await?;

        tb.broker::<RedisBroker>()
            .published::<Receipt>("receipts")
            .assert_called_once()
            .with(&Receipt { order_id: 12 });
        tb.shutdown().await?;
        Ok(())
    }
}
