//! Two handlers on one consumer group: a fresh-tail worker plus an `XAUTOCLAIM` recovery handler.
//!
//! `RedisStream::new(key)` reads new entries off the tail; `RedisStream::reclaim(key, min_idle)`
//! reads entries another consumer fetched but died before acking (idle at least `min_idle`). Running
//! both against the same group is the "two handlers per group" pattern: normal processing on one,
//! crash recovery on the other.
//!
//! `min_idle` must exceed the longest legitimate handler runtime, or a healthy consumer's in-flight
//! message gets reclaimed and processed twice.
//!
//! ```text
//! cargo run --example fred_reclaim --features macros,json -- run
//! ```

use std::time::Duration;

use ruststream::runtime::{AppInfo, HandlerResult, RustStream};
use ruststream::subscriber;
use ruststream_fred::{RedisBroker, RedisStream};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

// --8<-- [start:worker]
// The descriptor sits directly in the decorator: a fresh-tail consumer on the `workers` group.
#[subscriber(RedisStream::new("orders").group("workers"))]
async fn handle(order: &Order) -> HandlerResult {
    println!("processing order {}", order.id);
    HandlerResult::Ack
}
// --8<-- [end:worker]

// --8<-- [start:reclaim]
// A recovery handler for the same group: reclaims entries left pending for over 30s.
#[subscriber(RedisStream::reclaim("orders", Duration::from_secs(30)).group("workers"))]
async fn recover(order: &Order) -> HandlerResult {
    println!("recovering order {}", order.id);
    HandlerResult::Ack
}
// --8<-- [end:reclaim]

#[ruststream::app]
fn app() -> RustStream {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            b.include(handle);
            b.include(recover);
        },
    )
}
