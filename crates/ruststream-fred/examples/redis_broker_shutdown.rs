//! Shutdown: the runtime closes the broker's connections when the service stops, after the
//! handlers settled what they were holding.
//!
//! ```text
//! cargo test --example redis_broker_shutdown --features testing
//! ```

use std::convert::Infallible;

use serde::{Deserialize, Serialize};

use ruststream_fred::stream::prelude::*;

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "shipments")]
struct Shipment {
    order_id: u64,
}

#[subscriber(RedisStream::new("orders").group("workers"), reply)]
async fn ship(order: &Order) -> Shipment {
    Shipment { order_id: order.id }
}

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App<State = ()> {
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .with_broker(RedisBroker::standalone("redis://localhost:6379"), |b| {
            b.include(ship);
        })
        // Runs once every connection of the broker is closed.
        .after_shutdown(async move |_state| {
            tracing::info!("redis connections closed");
            Ok::<_, Infallible>(())
        })
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_fred::stream::prelude::*;

    use super::{Order, Shipment, app};

    #[tokio::test]
    async fn the_service_ships_then_shuts_down() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<RedisBroker>()
            .message(&Order { id: 1 })
            .publish()
            .await?;

        tb.broker::<RedisBroker>()
            .published::<Shipment>("shipments")
            .assert_called_once()
            .with(&Shipment { order_id: 1 });
        tb.shutdown().await?;
        Ok(())
    }
}
