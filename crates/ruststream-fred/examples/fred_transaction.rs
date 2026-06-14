//! Transactional publishing: a batch handler's replies are flushed as one pipeline on commit.
//!
//! On standalone and sentinel the stream publisher implements `TransactionalPublisher`. The
//! idiomatic way to use it is a batch-publishing handler wired with a `.transactional()` publisher:
//! every reply the handler returns is buffered and committed atomically, in order, through a single
//! `fred` pipeline (an `Err` publishes nothing and settles the batch). Cluster does not support it,
//! because buffered keys may live on different nodes.
//!
//! ```text
//! cargo run --example fred_transaction --features macros,json -- run
//! ```

use ruststream::runtime::{AppInfo, HandlerResult, RustStream, TypedPublisher};
use ruststream::subscriber;
use ruststream_fred::RedisBroker;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
struct Order {
    id: u64,
}

// --8<-- [start:batch]
// A batch-publishing handler: each reply in the returned Vec is published to `processed`, all
// committed atomically when the transactional publisher commits.
#[subscriber(batch("orders"), publish("processed"))]
async fn process(orders: &[Order]) -> Result<Vec<Order>, HandlerResult> {
    if orders.is_empty() {
        return Err(HandlerResult::drop());
    }
    Ok(orders.iter().map(|o| Order { id: o.id }).collect())
}
// --8<-- [end:batch]

#[ruststream::app]
fn app() -> RustStream {
    let broker = RedisBroker::standalone("redis://localhost:6379").default_group("workers");
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        // --8<-- [start:mount]
        // .transactional() uses RedisPublisher's TransactionalPublisher impl: the batch's replies
        // are buffered and flushed as one pipeline on commit.
        let processed = TypedPublisher::new(b.broker().publisher()).transactional();
        b.include_batch_publishing(process, processed);
        // --8<-- [end:mount]
    })
}
