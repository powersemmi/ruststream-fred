//! Transactional publishing on Redis, in both framework kinds.
//!
//! On standalone and sentinel the stream publisher carries the borrowed kind
//! (`TransactionalPublisher`, one transaction on the handle) and the owned kind
//! (`OwnedTransactions`, a buffer-owning value per call). Both commit the same way: the buffer is
//! flushed as one `MULTI` / `EXEC` block, so subscribers see the whole batch or none of it. The
//! idiomatic use of the borrowed kind is a batch-publishing handler wired with a `.transactional()`
//! publisher: every reply the handler returns is buffered and committed together, in order (an
//! `Err` publishes nothing and settles the batch). The owned kind suits publishes a handler does
//! not reply with, several of which may be in flight at once. Cluster supports neither, because a
//! `MULTI` block cannot span hash slots.
//!
//! ```text
//! cargo run --example fred_transaction --features macros,json -- run
//! ```

use ruststream::runtime::{App, AppInfo, HandlerResult, RustStream, TypedPublisher};
use ruststream::{OutgoingMessage, OwnedTransactions, Transaction, subscriber};
use ruststream_fred::{RedisBroker, RedisPublish};
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
fn app() -> impl App {
    let broker = RedisBroker::standalone("redis://localhost:6379").default_group("workers");
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        // --8<-- [start:mount]
        // .transactional() requires the policy's live form to be transactional, which
        // RedisPublish's is on standalone and sentinel: the batch's replies are buffered and
        // committed as one MULTI / EXEC block.
        b.include(process)
            .publisher(TypedPublisher::new(RedisPublish).transactional());
        // --8<-- [end:mount]

        // --8<-- [start:owned]
        // The owned kind: every `transaction()` call hands back a value owning its buffer, so
        // several can be open on one publisher at once and settling one never touches another.
        // `after_startup` is where a service makes its first publishes, once the broker is
        // connected.
        b.after_startup(RedisPublish, async move |publisher| {
            let mut seed = publisher.transaction().await?;
            seed.publish(OutgoingMessage::new("processed", br#"{"id":0}"#.as_slice()))
                .await?;
            // Commit flushes the buffer as one MULTI / EXEC block.
            seed.commit().await
        });
        // --8<-- [end:owned]
    })
}
