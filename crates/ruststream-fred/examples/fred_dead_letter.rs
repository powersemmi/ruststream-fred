//! Dead-letter routing and a poison cap on Redis Streams: a message that keeps failing is moved to
//! a dead-letter stream instead of being redelivered forever or silently dropped.
//!
//! ```text
//! cargo run --example fred_dead_letter --features macros,json -- run
//! ```
//!
//! Enqueue a poison order from another terminal (id 0 keeps failing until the cap dead-letters it):
//!
//! ```text
//! redis-cli XADD orders '*' _payload '{"id":0}'
//! ```

use ruststream::runtime::{AppInfo, HandlerResult, RustStream};
use ruststream::subscriber;
use ruststream_fred::{RedisBroker, RedisStream};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

// --8<-- [start:handler]
// Cap redeliveries at 5. On the 5th failed delivery (or an explicit drop) the message is copied to
// the "orders.dlq" stream, tagged with the reason, rather than retried forever or discarded.
#[subscriber(
    RedisStream::new("orders")
        .group("workers")
        .dead_letter("orders.dlq")
        .max_deliveries(5)
)]
async fn handle_order(order: &Order) -> HandlerResult {
    if order.id == 0 {
        // A poison message: nack to retry. Once the cap is reached it is dead-lettered for you.
        return HandlerResult::Nack { requeue: true };
    }
    println!("processed order {}", order.id);
    HandlerResult::Ack
}
// --8<-- [end:handler]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> RustStream {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            b.include(handle_order);
        },
    )
}
// --8<-- [end:app]
