//! The service of `examples/redis_streams_claiming.rs` written without the macros: the claiming
//! descriptor is the source `subscriber(..)` takes, and a body's `Err(retry)` leaves the entry
//! pending.
//!
//! ```text
//! REDIS_TEST_URL=redis://127.0.0.1:6379 cargo test --example manual_redis_streams_claiming --features testing
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
    in_stock: bool,
}

// `#[outgoing(name = "orders")]` by hand: the type fixes its destination.
impl OutgoingDestination for Order {
    type Form = FixedName;

    const DESTINATION: &'static str = "orders";
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

// --8<-- [start:claiming]
/// Fulfils new orders and the ones a worker took and never finished. An order out of stock stays
/// pending, and the subscription takes it back once it has been idle 30 seconds.
struct Fulfil;

impl Handle<Order> for Fulfil {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        if !order.in_stock {
            tracing::warn!(order.id, "out of stock, the order comes back");
            return ready(Err(HandlerOutcome::retry()));
        }
        tracing::info!(order.id, "order fulfilled");
        ready(Ok(()))
    }
}

fn app(url: &str) -> impl App {
    RustStream::new(AppInfo::new("fulfilment", "0.1.0")).with_broker(
        RedisBroker::standalone(url),
        |b| {
            let orders =
                RedisStream::claiming("orders", Duration::from_secs(30)).group("fulfilment");
            b.include(subscriber(orders, Fulfil).build());
        },
    )
}
// --8<-- [end:claiming]

// The scenarios are the tests below; the command in the header runs them.
fn main() {}

#[cfg(test)]
#[path = "../../tests/live/mod.rs"]
mod live;

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;

    use super::*;

    /// The address the in-process run builds the broker with; nothing dials it.
    const URL: &str = "redis://localhost:6379";

    #[tokio::test(start_paused = true)]
    async fn a_retried_order_comes_back_once_it_has_been_idle_min_idle()
    -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app(URL)).await?;
        let broker = tb.broker::<RedisBroker>();

        broker
            .message(&Order {
                id: 1,
                in_stock: false,
            })
            .publish()
            .await?;
        broker
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::retry());

        // A retry appends nothing: the entry waits in the pending entries list.
        tb.advance(Duration::from_secs(29)).await?;
        broker.subscriber("orders").assert_called(1);
        broker.published::<Order>("orders").assert_called(1);

        tb.advance(Duration::from_secs(1)).await?;
        broker.subscriber("orders").assert_called(2).with(&Order {
            id: 1,
            in_stock: false,
        });
        tb.shutdown().await?;
        Ok(())
    }

    /// `XREADGROUP ... CLAIM` arrived in Redis 8.4, so an older server refuses the subscription
    /// and the service does not start.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_older_server_refuses_to_start_the_service() {
        let Some(url) = live::url("REDIS_TEST_URL") else {
            return;
        };

        let refused = TestApp::start_live(app(&url))
            .await
            .expect_err("Redis 7 has no XREADGROUP ... CLAIM");
        let refused = format!("{refused:?}");
        assert!(
            refused.contains("RedisStream::claiming") && refused.contains("8.4"),
            "the refusal names the mode and the version it needs: {refused}"
        );
    }
}
