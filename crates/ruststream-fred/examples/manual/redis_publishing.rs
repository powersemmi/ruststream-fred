//! The service of `examples/redis_publishing.rs` written without `#[subscriber]`: the reply
//! policy is named at the mount site the same way.
//!
//! ```text
//! cargo test --example manual_redis_publishing --features testing
//! cargo run --example manual_redis_publishing -- run
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

// --8<-- [start:publish]
/// The stream a receipt goes to is a property of the type.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "receipts")]
struct Receipt {
    order_id: u64,
}

struct Confirm;

impl Handle<Order, Receipt> for Confirm {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Receipt, HandlerOutcome>> {
        tracing::info!(order.id, "order confirmed");
        ready(Ok(Receipt { order_id: order.id }))
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            let confirm = subscriber(RedisStream::new("orders").group("workers"), Confirm);
            // Each receipt is an `XADD` to the `receipts` stream.
            b.include(confirm.reply().build()).out_reply(Publish);
        },
    )
}
// --8<-- [end:publish]

fn main() -> ExitCode {
    run_main(app)
}

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
