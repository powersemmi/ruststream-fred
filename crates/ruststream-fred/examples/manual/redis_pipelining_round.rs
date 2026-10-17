//! The service of `examples/redis_pipelining_round.rs` written without `#[subscriber]`: the body
//! binds the slot it was handed to the delivery's round, and the mount puts the reply there with
//! the `InRound` transform.
//!
//! ```text
//! cargo test --example manual_redis_pipelining_round --features testing
//! cargo run --example manual_redis_pipelining_round -- run
//! ```

use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_fred::stream::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
    items: u32,
}

/// The order log: what happened to an order, in the order it happened.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "order-log")]
struct LogEntry {
    id: u64,
    step: Step,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[serde(rename_all = "lowercase")]
enum Step {
    Accepted,
    Confirmed,
}

// --8<-- [start:bind]
struct Accept;

impl<Log> Handle<Order, LogEntry, Outs<(Log,)>, PipelineContext> for Accept
where
    Log: OutEntry<DefaultSlot, Wire: Bindable>,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(Log,)>,
        ctx: &mut Context<'_, PipelineContext>,
    ) -> Result<LogEntry, HandlerOutcome> {
        // Bound to the round, the entry leaves with the acknowledgement, and not without it.
        let log = ctx.context(keys::Pipeline).bind(outs.get(DefaultSlot));
        let accepted = LogEntry {
            id: order.id,
            step: Step::Accepted,
        };
        if log.message(&accepted).publish().await.is_err() {
            return Err(HandlerOutcome::retry());
        }
        if order.items == 0 {
            tracing::warn!(order.id, "an order without items is dropped");
            return Err(HandlerOutcome::drop());
        }
        Ok(LogEntry {
            id: order.id,
            step: Step::Confirmed,
        })
    }
}
// --8<-- [end:bind]

// --8<-- [start:in_round]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            let accept = subscriber(RedisStream::new("orders").group("workers"), Accept);
            // The reply joins the delivery's round after the bound entry, so the log keeps the
            // order the handler wrote them in.
            b.include(accept.pipeline().reply().build())
                .out_reply(Publish)
                .transform(InRound)
                .out(DefaultSlot, Publish)
                .build();
        },
    )
}
// --8<-- [end:in_round]

fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_fred::stream::prelude::*;

    use super::{LogEntry, Order, Step, app};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_acknowledged_order_logs_both_steps_in_order() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<RedisBroker>()
            .message(&Order { id: 5, items: 1 })
            .publish()
            .await?;

        tb.broker::<RedisBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        let steps: Vec<Step> = tb
            .broker::<RedisBroker>()
            .published::<LogEntry>("order-log")
            .assert_called(2)
            .decoded()
            .into_iter()
            .map(|entry| entry.step)
            .collect();
        assert_eq!(steps, [Step::Accepted, Step::Confirmed]);
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_dropped_order_logs_nothing() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<RedisBroker>()
            .message(&Order { id: 6, items: 0 })
            .publish()
            .await?;

        tb.broker::<RedisBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.broker::<RedisBroker>()
            .published::<LogEntry>("order-log")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }
}
