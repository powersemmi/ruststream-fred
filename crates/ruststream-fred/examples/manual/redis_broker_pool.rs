//! The service of `examples/redis_broker_pool.rs` without the macros: the handler is a named type
//! and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_redis_broker_pool --features testing
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream_fred::stream::prelude::*;

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
}

// `#[outgoing(name = "orders")]` by hand: an order names the stream it is added to.
impl OutgoingDestination for Order {
    type Form = FixedName;
    const DESTINATION: &'static str = "orders";
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Receipt {
    order_id: u64,
}

// `#[outgoing(name = "receipts")]` by hand: a receipt names the stream it is added to.
impl OutgoingDestination for Receipt {
    type Form = FixedName;
    const DESTINATION: &'static str = "receipts";
}

impl MessageHeaders for Receipt {
    type Contract = NoHeaders;
}

struct Price;

impl Handle<Order, Receipt> for Price {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Receipt, HandlerOutcome>> {
        tracing::info!(order.id, "order priced");
        ready(Ok(Receipt { order_id: order.id }))
    }
}

fn service(broker: RedisBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(
            subscriber(RedisStream::new("orders").group("workers"), Price)
                .reply()
                .build(),
        );
    })
}

// --8<-- [start:app]
/// Sixteen pooled connections for the replies and acknowledgements of a busy service.
fn app() -> impl App<State = ()> {
    service(RedisBroker::standalone("redis://localhost:6379").pool(16))
}
// --8<-- [end:app]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_fred::stream::prelude::*;

    use super::{Order, Receipt, app, service};

    #[tokio::test]
    async fn a_reply_leaves_through_the_pool() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<RedisBroker>()
            .message(&Order { id: 4 })
            .publish()
            .await?;

        tb.broker::<RedisBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.broker::<RedisBroker>()
            .published::<Receipt>("receipts")
            .assert_called_once()
            .with(&Receipt { order_id: 4 });
        Ok(())
    }

    #[tokio::test]
    async fn a_pool_of_no_connections_does_not_start() {
        let broker = RedisBroker::standalone("redis://localhost:6379").pool(0);

        let err = TestApp::start(service(broker))
            .await
            .err()
            .map(|err| format!("{err:?}"));
        assert!(
            err.as_deref().is_some_and(|err| err.contains("empty")),
            "{err:?}"
        );
    }
}
