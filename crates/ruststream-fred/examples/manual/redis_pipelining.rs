//! The service of `examples/redis_pipelining.rs` written without `#[subscriber]`: a body typed
//! over `PipelineContext` reads the delivery's round off its context and queues a command into it.
//!
//! ```text
//! cargo test --example manual_redis_pipelining --features testing
//! cargo run --example manual_redis_pipelining -- run
//! ```

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
    items: u32,
}

// --8<-- [start:pipeline]
/// Records an accepted order on the `orders:accepted` list, in the same round trip as its `XACK`.
struct Accept;

impl Handle<Order, (), (), PipelineContext> for Accept {
    async fn handle(
        &self,
        order: &Order,
        _outs: &(),
        ctx: &mut Context<'_, PipelineContext>,
    ) -> Result<(), HandlerOutcome> {
        let pipeline = ctx.context(keys::Pipeline);
        if pipeline
            .lpush("orders:accepted", order.id.to_string())
            .await
            .is_err()
        {
            return Err(HandlerOutcome::retry());
        }
        if order.items == 0 {
            // The delivery is dropped, and the push queued above goes with it.
            tracing::warn!(order.id, "an order without items is dropped");
            return Err(HandlerOutcome::drop());
        }
        Ok(())
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            let accept = subscriber(RedisStream::new("orders").group("workers"), Accept);
            b.include(accept.pipeline().build());
        },
    )
}
// --8<-- [end:pipeline]

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
    async fn an_acknowledged_order_leaves_its_push() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<RedisBroker>()
            .message(&Order { id: 7, items: 2 })
            .publish()
            .await?;

        tb.broker::<RedisBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.broker::<RedisBroker>()
            .published::<u64>("orders:accepted")
            .assert_called_once()
            .with(&7);
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_dropped_order_leaves_nothing_it_queued() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<RedisBroker>()
            .message(&Order { id: 8, items: 0 })
            .publish()
            .await?;

        tb.broker::<RedisBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.broker::<RedisBroker>()
            .published::<u64>("orders:accepted")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }
}
