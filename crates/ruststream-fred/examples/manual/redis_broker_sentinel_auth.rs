//! The service of `examples/redis_broker_sentinel_auth.rs` without the macros: the handler is a
//! named type and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_redis_broker_sentinel_auth --features testing,sentinel-auth
//! ```

use std::env;
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

struct Take;

impl Handle<Order> for Take {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, "order taken");
        ready(Ok(()))
    }
}

fn service(broker: RedisBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(subscriber(RedisStream::new("orders").group("workers"), Take).build());
    })
}

// --8<-- [start:broker]
/// The data nodes check the user `worker`, the sentinels the user `monitor`.
fn broker(data_password: &str, sentinel_password: &str) -> RedisBroker {
    RedisBroker::sentinel(
        "mymaster",
        ["sentinel-1.internal:26379", "sentinel-2.internal:26379"],
    )
    .credentials("worker", data_password)
    .sentinel_credentials("monitor", sentinel_password)
}

fn app() -> impl App<State = ()> {
    let data_password = env::var("REDIS_PASSWORD").unwrap_or_default();
    let sentinel_password = env::var("SENTINEL_PASSWORD").unwrap_or_default();
    service(broker(&data_password, &sentinel_password))
}
// --8<-- [end:broker]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_fred::stream::prelude::*;

    use super::{Order, app};

    #[tokio::test]
    async fn a_service_with_sentinel_credentials_takes_an_order() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<RedisBroker>()
            .message(&Order { id: 8 })
            .publish()
            .await?;

        tb.broker::<RedisBroker>()
            .subscriber("orders")
            .assert_called_once()
            .with(&Order { id: 8 })
            .settled(HandlerOutcome::ack());
        Ok(())
    }
}
