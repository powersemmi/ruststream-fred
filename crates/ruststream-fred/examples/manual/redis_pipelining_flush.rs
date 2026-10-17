//! The service of `examples/redis_pipelining_flush.rs` written without `#[subscriber]`: a batch
//! body typed over `PipelineContext` queues its commands into the batch's segment.
//!
//! ```text
//! cargo test --example manual_redis_pipelining_flush --features testing
//! cargo run --example manual_redis_pipelining_flush -- run
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

// --8<-- [start:batch]
/// Records every order of a batch; one order without items drops the batch and its pushes.
struct AcceptAll;

impl Handle<[Order], (), (), PipelineContext> for AcceptAll {
    async fn handle(
        &self,
        orders: &[Order],
        _outs: &(),
        ctx: &mut Context<'_, PipelineContext>,
    ) -> Result<(), Vec<HandlerOutcome>> {
        let pipeline = ctx.context(keys::Pipeline);
        for order in orders {
            if pipeline
                .lpush("orders:accepted", order.id.to_string())
                .await
                .is_err()
            {
                return Err(orders.iter().map(|_| HandlerOutcome::retry()).collect());
            }
        }
        if orders.iter().any(|order| order.items == 0) {
            tracing::warn!(
                orders = orders.len(),
                "a batch with an empty order is dropped"
            );
            return Err(orders.iter().map(|_| HandlerOutcome::drop()).collect());
        }
        Ok(())
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            let accept_all = subscriber(RedisStream::new("orders").group("workers"), AcceptAll);
            b.include(accept_all.batch(nonzero!(64)).pipeline().build());
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

    use super::{Order, app};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_acknowledged_batch_sends_every_push() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        for id in 1..=3 {
            tb.broker::<RedisBroker>()
                .message(&Order { id, items: 1 })
                .publish()
                .await?;
        }

        let pushed: Vec<u64> = tb
            .broker::<RedisBroker>()
            .published::<u64>("orders:accepted")
            .assert_called(3)
            .decoded();
        assert_eq!(pushed, [1, 2, 3]);
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_dropped_batch_sends_nothing_it_queued() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<RedisBroker>()
            .message(&Order { id: 4, items: 0 })
            .publish()
            .await?;

        tb.broker::<RedisBroker>()
            .subscriber("orders")
            .assert_called_once();
        tb.broker::<RedisBroker>()
            .published::<u64>("orders:accepted")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }
}
