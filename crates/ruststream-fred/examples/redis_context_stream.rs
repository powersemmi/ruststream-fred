//! What a stream delivery carries besides its payload and headers: the entry id it was read at,
//! the position that redelivers it, the consumer group and the group's reposition handle, each
//! read off a `StreamContext` by its key.
//!
//! ```text
//! cargo test --example redis_context_stream --features testing
//! cargo run --example redis_context_stream -- run
//! ```

use ruststream_fred::stream::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

/// Where an order sat in the stream, written to the order trail.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "order-trail")]
struct Trail {
    order_id: u64,
    entry_id: String,
    group: String,
}

// --8<-- [start:context]
/// Reads the entry id and the consumer group off the delivery's context.
#[subscriber(RedisStream::new("orders").group("audit"), reply)]
async fn trace(order: &Order, ctx: &mut Context<'_, StreamContext>) -> Trail {
    let entry_id = ctx.context(keys::EntryId);
    let group = ctx.context(keys::ConsumerGroup);
    tracing::info!(order.id, %entry_id, group, "order traced");
    Trail {
        order_id: order.id,
        entry_id: entry_id.to_string(),
        group: group.to_owned(),
    }
}
// --8<-- [end:context]

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("audit", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            b.include(trace).out_reply(Publish);
        },
    )
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
