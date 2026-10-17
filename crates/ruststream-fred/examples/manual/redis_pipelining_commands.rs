//! The service of `examples/redis_pipelining_commands.rs` written without `#[subscriber]`: the
//! bodies read the delivery's round off a `PipelineContext` and queue the same commands.
//!
//! ```text
//! cargo test --example manual_redis_pipelining_commands --features testing
//! REDIS_TEST_URL=redis://127.0.0.1:6379 RUSTSTREAM_REQUIRE_LIVE=1 \
//!     cargo test --example manual_redis_pipelining_commands --features testing
//! cargo run --example manual_redis_pipelining_commands -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_fred::prelude::*;
use serde::{Deserialize, Serialize};

#[cfg(test)]
#[path = "../../tests/live/mod.rs"]
mod live;

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
}

// --8<-- [start:commands]
/// Puts the order on the shipping queue and counts it, both in the delivery's round.
struct Ship;

impl Handle<Order, (), (), PipelineContext> for Ship {
    async fn handle(
        &self,
        order: &Order,
        _outs: &(),
        ctx: &mut Context<'_, PipelineContext>,
    ) -> Result<(), HandlerOutcome> {
        let pipeline = ctx.context(keys::Pipeline);
        // `Ok` means queued: the commands run after this handler acknowledges the delivery.
        if pipeline
            .lpush("shipments", order.id.to_string())
            .await
            .is_err()
            || pipeline
                .hincrby("orders:totals", "shipped", 1)
                .await
                .is_err()
        {
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}
// --8<-- [end:commands]

// --8<-- [start:script]
/// The same two writes as one Lua script, so no client sees one without the other.
const SHIP_AND_COUNT: &str = "redis.call('HINCRBY', KEYS[2], 'shipped', 1) \
                              return redis.call('LPUSH', KEYS[1], ARGV[1])";

struct ShipFirst;

impl Handle<Order, (), (), PipelineContext> for ShipFirst {
    async fn handle(
        &self,
        order: &Order,
        _outs: &(),
        ctx: &mut Context<'_, PipelineContext>,
    ) -> Result<(), HandlerOutcome> {
        let touched = vec!["shipments", "orders:totals"];
        let pipeline = ctx.context(keys::Pipeline);
        if pipeline
            .eval(SHIP_AND_COUNT, touched, vec![order.id.to_string()])
            .await
            .is_err()
        {
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}
// --8<-- [end:script]

/// Takes an order id off the shipping queue.
struct Dispatch;

impl Handle<u64> for Dispatch {
    fn handle(
        &self,
        id: &u64,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(id, "shipment dispatched");
        ready(Ok(()))
    }
}

/// The service on the Redis server at `url`.
fn app(url: &str) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("shipping", "0.1.0")).with_broker(
        RedisBroker::standalone(url),
        |b| {
            let ship = subscriber(RedisStream::new("orders").group("workers"), Ship);
            b.include(ship.pipeline().build());
            let ship_first = subscriber(
                RedisStream::new("orders.priority").group("workers"),
                ShipFirst,
            );
            b.include(ship_first.pipeline().build());
            b.include(subscriber(RedisList::new("shipments"), Dispatch).build());
        },
    )
}

fn main() -> ExitCode {
    run_main(|| app("redis://localhost:6379"))
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_fred::prelude::*;

    use super::{Order, app, live};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_queued_push_reaches_the_shipping_queue() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app("redis://localhost:6379")).await?;
        tb.broker::<RedisBroker>()
            .message(&Order { id: 7 })
            .to("orders")
            .publish()
            .await?;
        tb.settle().await?;

        tb.broker::<RedisBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.broker::<RedisBroker>()
            .subscriber("shipments")
            .assert_called_once()
            .with(&7_u64);
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_queued_script_reaches_the_shipping_queue() -> Result<(), Box<dyn Error>> {
        let Some(url) = live::url("REDIS_TEST_URL") else {
            return Ok(());
        };
        let tb = TestApp::start_live(app(&url)).await?;
        tb.broker::<RedisBroker>()
            .message(&Order { id: 9 })
            .to("orders.priority")
            .publish()
            .await?;
        tb.settle().await?;

        tb.broker::<RedisBroker>()
            .subscriber("orders.priority")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.broker::<RedisBroker>()
            .subscriber("shipments")
            .assert_called_once()
            .with(&9_u64);
        tb.shutdown().await?;
        Ok(())
    }
}
