//! The service of `examples/redis_context_stream.rs` written without `#[subscriber]`: a body
//! typed over `StreamContext` reads each field off its context by its key.
//!
//! ```text
//! cargo test --example manual_redis_context_stream --features testing
//! cargo run --example manual_redis_context_stream -- run
//! ```

use std::future::{Future, ready};
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
}

/// Where an order sat in the stream, written to the order trail.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "order-trail")]
struct Trail {
    order_id: u64,
    entry_id: String,
    group: String,
}

// --8<-- [start:context]
/// Reads the entry id and the consumer group off the delivery's context.
struct Trace;

impl Handle<Order, Trail, (), StreamContext> for Trace {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        ctx: &mut Context<'_, StreamContext>,
    ) -> impl Future<Output = Result<Trail, HandlerOutcome>> {
        let entry_id = ctx.context(keys::EntryId);
        let group = ctx.context(keys::ConsumerGroup);
        tracing::info!(order.id, %entry_id, group, "order traced");
        ready(Ok(Trail {
            order_id: order.id,
            entry_id: entry_id.to_string(),
            group: group.to_owned(),
        }))
    }
}
// --8<-- [end:context]

fn app() -> impl App {
    RustStream::new(AppInfo::new("audit", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            let trace = subscriber(RedisStream::new("orders").group("audit"), Trace);
            b.include(trace.reply().build()).out_reply(Publish);
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
    use ruststream_fred::EntryId;
    use ruststream_fred::stream::prelude::*;

    use super::{Order, Trail, app};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn each_order_records_its_entry_and_group() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        for id in [1, 2] {
            tb.broker::<RedisBroker>()
                .message(&Order { id })
                .publish()
                .await?;
        }

        let trails = tb
            .broker::<RedisBroker>()
            .published::<Trail>("order-trail")
            .assert_called(2)
            .decoded();
        let read: Vec<(u64, &str)> = trails
            .iter()
            .map(|trail| (trail.order_id, trail.group.as_str()))
            .collect();
        assert_eq!(read, [(1, "audit"), (2, "audit")]);
        let first: EntryId = trails[0].entry_id.parse()?;
        let second: EntryId = trails[1].entry_id.parse()?;
        assert!(first < second, "{first} is not before {second}");
        tb.shutdown().await?;
        Ok(())
    }
}
