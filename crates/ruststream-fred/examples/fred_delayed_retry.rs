//! Durable delayed retry on Redis Streams: a handler that asks for a delayed redelivery backs it
//! with a ZSET delay queue, so the retry survives a process crash rather than being lost.
//!
//! ```text
//! cargo run --example fred_delayed_retry --features macros,json -- run
//! ```
//!
//! Enqueue an order from another terminal (id 0 forces the delayed retry):
//!
//! ```text
//! redis-cli XADD orders '*' _payload '{"id":0}'
//! ```

use std::time::Duration;

use ruststream::runtime::{AppInfo, HandlerResult, RustStream};
use ruststream::subscriber;
use ruststream_fred::{DelayedRetry, RedisBroker, RedisStream};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

// --8<-- [start:handler]
// On a transient failure the handler asks for a delayed retry. The delay queue is the named ZSET,
// so the redelivery is durable: it survives a crash between the failure and the retry firing.
#[subscriber(
    RedisStream::new("orders")
        .group("workers")
        .delayed_retry(DelayedRetry::DurableZset { key: "orders.delayed".to_owned(), ttl: None })
)]
async fn handle_order(order: &Order) -> HandlerResult {
    if order.id == 0 {
        // Park the message in the ZSET for 30s instead of blocking the worker or busy-requeuing.
        return HandlerResult::retry_after(Duration::from_secs(30));
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
