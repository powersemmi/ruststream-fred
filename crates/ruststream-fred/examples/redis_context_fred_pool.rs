//! `Ctx<keys::FredPool>` hands a handler the broker's `fred` connection pool, for a command whose
//! answer the handler needs now.
//!
//! ```text
//! cargo test --example redis_context_fred_pool --features testing
//! cargo run --example redis_context_fred_pool -- run
//! ```

use fred::interfaces::KeysInterface;
use ruststream_fred::stream::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

/// An order with its place in the day's sequence.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "numbered-orders")]
struct Numbered {
    order_id: u64,
    number: i64,
}

// --8<-- [start:fred_pool]
/// Numbers each order with `INCR`, whose answer the reply carries.
#[subscriber(RedisStream::new("orders").group("numbering"), reply)]
async fn assign_number(
    order: &Order,
    Ctx(pool): Ctx<keys::FredPool>,
) -> Result<Numbered, HandlerOutcome> {
    match pool.incr::<i64, _>("orders:sequence").await {
        Ok(number) => Ok(Numbered {
            order_id: order.id,
            number,
        }),
        Err(err) => {
            tracing::warn!(order.id, %err, "the sequence is out of reach");
            Err(HandlerOutcome::retry())
        }
    }
}
// --8<-- [end:fred_pool]

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("numbering", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            b.include(assign_number).out_reply(Publish);
        },
    )
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_fred::stream::prelude::*;

    use super::{Numbered, Order, app};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn each_order_gets_the_next_number() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        for id in [10, 11] {
            tb.broker::<RedisBroker>()
                .message(&Order { id })
                .publish()
                .await?;
        }

        let numbered = tb
            .broker::<RedisBroker>()
            .published::<Numbered>("numbered-orders")
            .assert_called(2)
            .decoded();
        assert_eq!(
            numbered,
            [
                Numbered {
                    order_id: 10,
                    number: 1
                },
                Numbered {
                    order_id: 11,
                    number: 2
                },
            ],
        );
        tb.shutdown().await?;
        Ok(())
    }
}
