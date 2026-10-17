//! The service of `examples/redis_context_fred_pool.rs` written without `#[subscriber]`: a body
//! typed over `PoolContext` reads the pool off its context.
//!
//! ```text
//! cargo test --example manual_redis_context_fred_pool --features testing
//! cargo run --example manual_redis_context_fred_pool -- run
//! ```

use std::process::ExitCode;

use fred::interfaces::KeysInterface;
use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_fred::stream::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

/// An order with its place in the day's sequence.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "numbered-orders")]
struct Numbered {
    order_id: u64,
    number: i64,
}

// --8<-- [start:fred_pool]
/// Numbers each order with `INCR`, whose answer the reply carries.
struct AssignNumber;

impl Handle<Order, Numbered, (), PoolContext> for AssignNumber {
    async fn handle(
        &self,
        order: &Order,
        _outs: &(),
        ctx: &mut Context<'_, PoolContext>,
    ) -> Result<Numbered, HandlerOutcome> {
        let pool = ctx.context(keys::FredPool);
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
}
// --8<-- [end:fred_pool]

fn app() -> impl App {
    RustStream::new(AppInfo::new("numbering", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            let assign = subscriber(RedisStream::new("orders").group("numbering"), AssignNumber);
            b.include(assign.reply().build()).out_reply(Publish);
        },
    )
}

fn main() -> ExitCode {
    run_main(app)
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
