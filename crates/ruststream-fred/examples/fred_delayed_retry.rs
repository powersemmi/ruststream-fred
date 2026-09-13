//! Delayed retry on Redis Streams: the deferred copy the runtime publishes when the delay is up,
//! and the ZSET delay queue that makes the same retry survive a process crash.
//!
//! ```text
//! cargo run --example fred_delayed_retry --features macros,json -- run
//! ```
//!
//! Enqueue an order from another terminal (id 0 forces the delayed retry):
//!
//! ```text
//! redis-cli XADD orders '*' _payload '{"id":0}'
//! redis-cli XADD billing '*' _payload '{"id":0}'
//! ```

use std::time::Duration;

use ruststream_fred::stream::prelude::*;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

// --8<-- [start:deferred]
// A plain subscription has no per-message delay of its own, so the runtime serves the delay with a
// copy it publishes back to the stream once the delay is up.
#[subscriber(RedisStream::new("billing").group("workers"))]
async fn bill_order(order: &Order) -> HandlerOutcome {
    if order.id == 0 {
        return HandlerOutcome::retry_after(Duration::from_secs(30));
    }
    println!("billed order {}", order.id);
    HandlerOutcome::ack()
}
// --8<-- [end:deferred]

// --8<-- [start:handler]
// On a transient failure the handler asks for a delayed retry. The delay queue is the named ZSET,
// so the redelivery is durable: it survives a crash between the failure and the retry firing.
#[subscriber(
    RedisStream::new("orders")
        .group("workers")
        .delayed_retry(DelayedRetry::DurableZset { key: "orders.delayed".to_owned(), ttl: None })
)]
async fn handle_order(order: &Order) -> HandlerOutcome {
    if order.id == 0 {
        // Park the message in the ZSET for 30s instead of blocking the worker or busy-requeuing.
        return HandlerOutcome::retry_after(Duration::from_secs(30));
    }
    println!("processed order {}", order.id);
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            // `out_retry` names the publisher the deferred copy leaves through. Without it the
            // delay is dropped and the message is requeued at once.
            b.include(bill_order).out_retry(Publish);
            // The ZSET queue carries the delay itself, so this registration needs no publisher.
            b.include(handle_order);
        },
    )
}
// --8<-- [end:app]
