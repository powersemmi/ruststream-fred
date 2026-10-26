//! What a pipelined handler queues: `fred`'s command methods, and Lua through `eval`. A queued
//! command runs after the handler returns, with the delivery's `XACK`; the handler never reads its
//! reply.
//!
//! The in-process Redis runs no scripts, so the script half is proven against a server:
//!
//! ```text
//! cargo test --example redis_pipelining_commands --features testing
//! REDIS_TEST_URL=redis://127.0.0.1:6379 RUSTSTREAM_REQUIRE_LIVE=1 \
//!     cargo test --example redis_pipelining_commands --features testing
//! cargo run --example redis_pipelining_commands -- run
//! ```

use ruststream_fred::prelude::*;
use serde::{Deserialize, Serialize};

#[cfg(test)]
#[path = "../tests/live/mod.rs"]
mod live;

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

// --8<-- [start:commands]
/// Puts the order on the shipping queue and counts it, both in the delivery's round.
#[subscriber(RedisStream::new("orders").group("workers"))]
async fn ship(order: &Order, Ctx(pipeline): Ctx<keys::Pipeline>) -> HandlerOutcome {
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
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}
// --8<-- [end:commands]

// --8<-- [start:script]
/// The same two writes as one Lua script, so no client sees one without the other.
const SHIP_AND_COUNT: &str = "redis.call('HINCRBY', KEYS[2], 'shipped', 1) \
                              return redis.call('LPUSH', KEYS[1], ARGV[1])";

#[subscriber(RedisStream::new("orders.priority").group("workers"))]
async fn ship_first(order: &Order, Ctx(pipeline): Ctx<keys::Pipeline>) -> HandlerOutcome {
    let touched = vec!["shipments", "orders:totals"];
    if pipeline
        .eval(SHIP_AND_COUNT, touched, vec![order.id.to_string()])
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}
// --8<-- [end:script]

/// Takes an order id off the shipping queue.
#[subscriber(RedisList::new("shipments"))]
async fn dispatch(id: &u64) -> HandlerOutcome {
    tracing::info!(id, "shipment dispatched");
    HandlerOutcome::ack()
}

/// The service on the Redis server at `url`.
fn app(url: &str) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("shipping", "0.1.0")).with_broker(
        RedisBroker::standalone(url),
        |b| {
            b.include(ship.pipeline());
            b.include(ship_first.pipeline());
            b.include(dispatch);
        },
    )
}

#[ruststream::app]
fn service() -> impl App<State = ()> {
    app("redis://localhost:6379")
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use fred::interfaces::{HashesInterface, KeysInterface};
    use ruststream::ConnectedBroker;
    use ruststream::testing::TestApp;
    use ruststream_fred::ConnectedRedisBroker;
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

    /// A second client on the server, as another program would hold, with the names the service
    /// reads emptied: a server kept between runs holds what earlier runs and other examples left on
    /// `orders` and `shipments`, and that backlog would reach the handlers of this one.
    async fn observer(url: &str) -> Result<ConnectedRedisBroker, Box<dyn Error>> {
        let broker = RedisBroker::standalone(url).connect().await?;
        let names = vec!["orders", "orders.priority", "shipments", "orders:totals"];
        let _: i64 = broker.pool_handle()?.del(names).await?;
        Ok(broker)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_queued_script_reaches_the_shipping_queue() -> Result<(), Box<dyn Error>> {
        let Some(url) = live::url("REDIS_TEST_URL") else {
            return Ok(());
        };
        let observer = observer(&url).await?;
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
        // The script's writes run with the delivery's `XACK`, so they are on the server once the
        // order settled. Its push is not a publish the live harness sees, so the pop of
        // `shipments` is read from the count the script made beside it, not waited for.
        let shipped: Option<i64> = observer
            .pool_handle()?
            .hget("orders:totals", "shipped")
            .await?;
        assert_eq!(shipped, Some(1));
        tb.shutdown().await?;
        observer.shutdown().await?;
        Ok(())
    }
}
