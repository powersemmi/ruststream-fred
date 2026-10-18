//! `default_group`: the consumer group a bare `#[subscriber("key")]` reads its stream through.
//!
//! ```text
//! cargo test --example redis_broker_default_group --features testing
//! ```

use serde::{Deserialize, Serialize};

use ruststream_fred::stream::prelude::*;

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

// --8<-- [start:app]
#[subscriber("orders")]
async fn take(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order taken");
    HandlerOutcome::ack()
}

fn service(broker: RedisBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(take);
    })
}

/// Every bare-name subscription of this broker reads in the group `workers`.
#[ruststream::app]
fn app() -> impl App<State = ()> {
    service(RedisBroker::standalone("redis://localhost:6379").default_group("workers"))
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_fred::stream::prelude::*;

    use super::{Order, app, service};

    #[tokio::test]
    async fn a_bare_name_reads_in_the_default_group() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<RedisBroker>()
            .message(&Order { id: 2 })
            .publish()
            .await?;

        tb.broker::<RedisBroker>()
            .subscriber("orders")
            .assert_called_once()
            .with(&Order { id: 2 })
            .settled(HandlerOutcome::ack());
        Ok(())
    }

    #[tokio::test]
    async fn a_bare_name_without_a_default_group_does_not_start() {
        let broker = RedisBroker::standalone("redis://localhost:6379");

        let err = TestApp::start(service(broker))
            .await
            .err()
            .map(|err| format!("{err:?}"));
        assert!(
            err.as_deref()
                .is_some_and(|err| err.contains("default_group")),
            "{err:?}"
        );
    }
}
