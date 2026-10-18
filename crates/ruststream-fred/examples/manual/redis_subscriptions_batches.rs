//! The service of `examples/redis_subscriptions_batches.rs` written without the macros: a body
//! over `[T]` with `.batch(n)` before `build()` is a batch handler on every form.
//!
//! ```text
//! cargo test --example manual_redis_subscriptions_batches --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::future::{Future, ready};

use ruststream::schemars::{self, JsonSchema};
use ruststream_fred::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
struct Order {
    id: u64,
}

// `#[derive(Outgoing)]` with no `name`, by hand: the call site names the key or the channel.
impl OutgoingDestination for Order {
    type Form = CallerName;
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

// --8<-- [start:stream]
/// Bills the orders one read returned.
struct Bill;

impl Handle<[Order]> for Bill {
    fn handle(
        &self,
        orders: &[Order],
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), Vec<HandlerOutcome>>> {
        tracing::info!(count = orders.len(), "orders billed");
        ready(Ok(()))
    }
}

fn app() -> impl App<State = ()> {
    let broker = RedisBroker::standalone("redis://localhost:6379");
    RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(broker, |b| {
        let orders = RedisStream::new("orders").group("billing");
        b.include(subscriber(orders, Bill).batch(nonzero!(16)).build());
        b.include(
            subscriber(RedisList::new("jobs"), Pack)
                .batch(nonzero!(16))
                .build(),
        );
        b.include(
            subscriber(RedisPubSub::new("clicks"), Count)
                .batch(nonzero!(16))
                .build(),
        );
    })
}
// --8<-- [end:stream]

// --8<-- [start:client]
/// Packs the jobs waiting on the list `jobs`.
struct Pack;

impl Handle<[Order]> for Pack {
    fn handle(
        &self,
        jobs: &[Order],
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), Vec<HandlerOutcome>>> {
        tracing::info!(count = jobs.len(), "jobs packed");
        ready(Ok(()))
    }
}

/// Counts the clicks heard on the channel `clicks`.
struct Count;

impl Handle<[Order]> for Count {
    fn handle(
        &self,
        clicks: &[Order],
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), Vec<HandlerOutcome>>> {
        tracing::info!(count = clicks.len(), "clicks counted");
        ready(Ok(()))
    }
}
// --8<-- [end:client]

// The scenarios are the tests below; the command in the header runs them.
fn main() {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use futures::future::join_all;
    use ruststream::testing::TestApp;

    use super::*;

    /// Publishes orders 1 to 3 to `name` at once, so they wait together for the next read.
    async fn three_at_once(tb: &TestApp<()>, name: &str) -> Result<(), Box<dyn Error>> {
        let orders: Vec<Order> = (1..=3).map(|id| Order { id }).collect();
        let broker = tb.broker::<RedisBroker>();
        for published in join_all(
            orders
                .iter()
                .map(|order| broker.message(order).to(name).publish()),
        )
        .await
        {
            published?;
        }
        Ok(())
    }

    fn ids(batches: Vec<Vec<Order>>) -> Vec<Vec<u64>> {
        batches
            .into_iter()
            .map(|batch| batch.into_iter().map(|order| order.id).collect())
            .collect()
    }

    #[tokio::test]
    async fn a_stream_batch_is_what_one_read_returned() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        three_at_once(&tb, "orders").await?;

        let batches = ids(tb.broker::<RedisBroker>().subscriber("orders").batches());
        assert_eq!(batches, [vec![1, 2, 3]]);
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_list_batch_takes_what_waits_on_the_list() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        three_at_once(&tb, "jobs").await?;

        let batches = ids(tb.broker::<RedisBroker>().subscriber("jobs").batches());
        assert_eq!(batches, [vec![1, 2, 3]]);
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_channel_batch_takes_what_the_subscription_heard() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        three_at_once(&tb, "clicks").await?;

        let batches = ids(tb.broker::<RedisBroker>().subscriber("clicks").batches());
        assert_eq!(batches, [vec![1, 2, 3]]);
        tb.shutdown().await?;
        Ok(())
    }
}
