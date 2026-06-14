//! A Redis Streams service: one `#[subscriber]` handler bound to a stream key, wired onto a
//! [`RedisBroker`].
//!
//! `RedisBroker::standalone` is synchronous and does no I/O, so the whole service fits the
//! `#[ruststream::app]` macro just like the in-memory examples. The runtime connects the broker once
//! at startup (`Broker::connect`) before opening subscriptions, and the generated binary understands
//! `run` and `asyncapi gen`.
//!
//! Redis Streams always read through a consumer group, so the bare-string subscriber form needs a
//! broker-wide default group (`.default_group`). For per-subscription control (fresh tail vs
//! reclaim, count, block) use the [`RedisStream`](ruststream_fred::RedisStream) descriptor instead.
//!
//! Start a Redis server first (`docker run -p 6379:6379 redis:7`), then:
//!
//! ```text
//! cargo run --example fred_streams --features macros,json -- run
//! ```
//!
//! Publish an order from another terminal with the Redis CLI:
//!
//! ```text
//! redis-cli XADD orders '*' _payload '{"id":1}'
//! ```

// --8<-- [start:handler]
use ruststream::runtime::{AppInfo, HandlerResult, RustStream};
use ruststream::subscriber;
use ruststream_fred::RedisBroker;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

#[subscriber("orders")]
async fn handle(order: &Order) -> HandlerResult {
    println!("got order {}", order.id);
    HandlerResult::Ack
}
// --8<-- [end:handler]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> RustStream {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379").default_group("workers"),
        |b| {
            b.include(handle);
        },
    )
}
// --8<-- [end:app]
