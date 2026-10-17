//! The connection pool: `pool(n)` sets how many connections publishes, settlements and handler
//! commands share; each subscription reads on a connection of its own besides.
//!
//! ```text
//! cargo test --example redis_broker_pool --features testing
//! ```

use serde::{Deserialize, Serialize};

use ruststream_fred::stream::prelude::*;

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "receipts")]
struct Receipt {
    order_id: u64,
}

#[subscriber(RedisStream::new("orders").group("workers"), reply)]
async fn price(order: &Order) -> Receipt {
    tracing::info!(order.id, "order priced");
    Receipt { order_id: order.id }
}

fn service(broker: RedisBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(price);
    })
}

// --8<-- [start:app]
/// Sixteen pooled connections for the replies and acknowledgements of a busy service.
#[ruststream::app]
fn app() -> impl App<State = ()> {
    service(RedisBroker::standalone("redis://localhost:6379").pool(16))
}
// --8<-- [end:app]

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
