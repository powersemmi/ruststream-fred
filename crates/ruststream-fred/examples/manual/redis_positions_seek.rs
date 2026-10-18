//! The service of `examples/redis_positions_seek.rs` written without the macros: a batch body on
//! the stream batch context reads the group's seek handle.
//!
//! ```text
//! cargo test --example manual_redis_positions_seek --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream::schemars::{self, JsonSchema};
use ruststream_fred::stream::prelude::*;
use serde::{Deserialize, Serialize};

/// An order; the producer sends id 0 to mark the orders queued behind it as void.
#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
struct Order {
    id: u64,
}

// `#[outgoing(name = "orders")]` by hand: the type fixes its destination.
impl OutgoingDestination for Order {
    type Form = FixedName;

    const DESTINATION: &'static str = "orders";
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

// --8<-- [start:seek]
/// Bills a batch of orders. A batch holding the void marker moves the group to the tail once it
/// is billed, so the orders queued behind the marker are never billed.
struct Bill;

impl Handle<[Order], (), (), StreamBatchContext> for Bill {
    async fn handle(
        &self,
        orders: &[Order],
        _outs: &(),
        ctx: &mut Context<'_, StreamBatchContext>,
    ) -> Result<(), Vec<HandlerOutcome>> {
        tracing::info!(count = orders.len(), "orders billed");
        if orders.iter().all(|order| order.id != 0) {
            return Ok(());
        }
        let group = ctx.context(keys::ConsumerGroup);
        tracing::info!(%group, "void marker, the group moves to the tail");
        ctx.context(keys::SeekHandle)
            .seek(RedisGroupPosition::end())
            .await
            .map_err(|_| orders.iter().map(|_| HandlerOutcome::retry()).collect())
    }
}

fn app() -> impl App {
    let broker = RedisBroker::standalone("redis://localhost:6379");
    RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(broker, |b| {
        let orders = RedisStream::new("orders").group("billing");
        b.include(subscriber(orders, Bill).batch(nonzero!(2)).build());
    })
}
// --8<-- [end:seek]

// The scenarios are the tests below; the command in the header runs them.
fn main() {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use futures::future::join_all;
    use ruststream::testing::TestApp;

    use super::*;

    #[tokio::test]
    async fn the_next_batch_starts_after_the_seek() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<RedisBroker>();

        // The first batch holds the marker; the two orders behind it wait in the stream.
        let queued: Vec<Order> = [1, 0, 2, 3].into_iter().map(|id| Order { id }).collect();
        for published in join_all(queued.iter().map(|order| broker.message(order).publish())).await
        {
            published?;
        }
        broker.message(&Order { id: 4 }).publish().await?;

        let batches: Vec<Vec<u64>> = broker
            .subscriber("orders")
            .batches::<Order>()
            .iter()
            .map(|batch| batch.iter().map(|order| order.id).collect())
            .collect();
        assert_eq!(batches, [vec![1, 0], vec![4]]);
        tb.shutdown().await?;
        Ok(())
    }
}
