//! Sentinel credentials: `sentinel_credentials` logs in to the sentinels with a pair of their own,
//! apart from the data nodes' `credentials`. The in-process mode runs no handshake, so the test
//! proves the service starts and works with both pairs set.
//!
//! ```text
//! cargo test --example redis_broker_sentinel_auth --features testing,sentinel-auth
//! ```

use std::env;

use serde::{Deserialize, Serialize};

use ruststream_fred::stream::prelude::*;

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

#[subscriber(RedisStream::new("orders").group("workers"))]
async fn take(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order taken");
    HandlerOutcome::ack()
}

fn service(broker: RedisBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(take);
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

#[ruststream::app]
fn app() -> impl App<State = ()> {
    let data_password = env::var("REDIS_PASSWORD").unwrap_or_default();
    let sentinel_password = env::var("SENTINEL_PASSWORD").unwrap_or_default();
    service(broker(&data_password, &sentinel_password))
}
// --8<-- [end:broker]

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
