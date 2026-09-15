//! One handler per consumer group on Redis 8.4: `XREADGROUP ... CLAIM` reads new entries and
//! re-reads the group's stale ones in the same call.
//!
//! `RedisStream::claiming(key, min_idle)` first takes over the entries idle at least `min_idle`,
//! longest idle first, then fills the rest of the read with fresh ones. Recovery needs no second
//! subscription, and the delivery the server hands over carries how long it had been pending and
//! how many attempts it has already survived.
//!
//! `min_idle` must exceed the longest legitimate handler runtime, or a message a healthy consumer
//! is still working on is taken away and processed twice.
//!
//! ```text
//! cargo run --example fred_claiming --features macros,json -- run
//! ```

use std::time::Duration;

use ruststream_fred::DELIVERY_COUNT_HEADER;
use ruststream_fred::stream::prelude::*;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

// --8<-- [start:claiming]
// One handler for both sets: new orders, and the ones a worker took and never finished.
#[subscriber(RedisStream::claiming("orders", Duration::from_secs(30)).group("workers"))]
async fn handle(order: &Order) -> HandlerOutcome {
    println!("processing order {}", order.id);
    HandlerOutcome::ack()
}
// --8<-- [end:claiming]

// --8<-- [start:cap]
// A retry leaves the entry pending, so the delivery count is how many attempts did not finish:
// zero on the first, one on the claim after it. A handler gives up by reading it.
#[subscriber(RedisStream::claiming("payments", Duration::from_secs(30)).group("workers"))]
async fn charge(order: &Order, ctx: &mut Context<'_>) -> HandlerOutcome {
    const MAX_ATTEMPTS: u64 = 3;

    let attempts = ctx
        .headers()
        .get_str(DELIVERY_COUNT_HEADER)
        .and_then(|count| count.parse::<u64>().ok())
        .unwrap_or(0);
    if attempts >= MAX_ATTEMPTS {
        println!("giving up on order {} after {attempts} attempts", order.id);
        return HandlerOutcome::drop();
    }
    println!("charging order {} (attempt {})", order.id, attempts + 1);
    HandlerOutcome::retry()
}
// --8<-- [end:cap]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            b.include(handle);
            b.include(charge);
        },
    )
}
// --8<-- [end:app]
