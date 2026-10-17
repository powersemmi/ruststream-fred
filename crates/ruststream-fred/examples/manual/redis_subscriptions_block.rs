//! The service of `examples/redis_subscriptions_block.rs` written without the macros: `block` on
//! the descriptor `subscriber(..)` takes, or the `block` step after `build()`.
//!
//! ```text
//! cargo test --example manual_redis_subscriptions_block --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::future::{Future, ready};
use std::time::Duration;

use ruststream::schemars::{self, JsonSchema};
use ruststream_fred::stream::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
struct Order {
    id: u64,
}

// `#[derive(Outgoing)]` with no `name`, by hand: the call site names the stream.
impl OutgoingDestination for Order {
    type Form = CallerName;
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

// --8<-- [start:block]
/// Places one order.
struct Place;

impl Handle<Order> for Place {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, "order placed");
        ready(Ok(()))
    }
}

/// Bills the orders of one read.
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

fn app() -> impl App {
    let broker = RedisBroker::standalone("redis://localhost:6379");
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        // Each read of `orders` waits at most one second on the server.
        let orders = RedisStream::new("orders")
            .group("workers")
            .block(Duration::from_secs(1));
        b.include(subscriber(orders, Place).build());
        let invoices = RedisStream::new("invoices").group("billing");
        b.include(
            subscriber(invoices, Bill)
                .batch(nonzero!(16))
                .build()
                .block(Duration::from_secs(2)),
        );
    })
}
// --8<-- [end:block]

// The scenarios are the tests below; the command in the header runs them.
fn main() {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;

    use super::*;

    #[tokio::test]
    async fn a_read_with_its_own_wait_delivers() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<RedisBroker>();

        broker
            .message(&Order { id: 1 })
            .to("orders")
            .publish()
            .await?;
        broker
            .message(&Order { id: 2 })
            .to("invoices")
            .publish()
            .await?;

        broker
            .subscriber("orders")
            .assert_called_once()
            .with(&Order { id: 1 })
            .settled(HandlerOutcome::ack());
        assert_eq!(
            broker.subscriber("invoices").batches::<Order>(),
            [vec![Order { id: 2 }]]
        );
        tb.shutdown().await?;
        Ok(())
    }
}
