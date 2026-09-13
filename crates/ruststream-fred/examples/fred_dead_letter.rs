//! Capping the retries on Redis Streams: a message that keeps failing goes to a dead-letter stream
//! instead of being redelivered forever or silently dropped.
//!
//! ```text
//! cargo run --example fred_dead_letter --features macros,json -- run
//! ```
//!
//! Enqueue a poison order from another terminal (id 0 keeps failing until the cap moves it):
//!
//! ```text
//! redis-cli XADD orders '*' _payload '{"id":0}'
//! ```

use ruststream_fred::stream::prelude::*;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

// --8<-- [start:handler]
#[subscriber(RedisStream::new("orders").group("workers"))]
async fn handle_order(order: &Order) -> HandlerOutcome {
    if order.id == 0 {
        // A poison message: ask for a retry. Once the attempts are spent it is carried away for
        // you, without the handler counting anything itself.
        return HandlerOutcome::retry();
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
            // Five deliveries per message, counting the first. The fifth failure sends it to the
            // "orders.dlq" stream as it arrived, payload and headers, instead of back to "orders".
            b.include(handle_order)
                .max_attempts(nonzero!(5u32))
                .dead_letter("orders.dlq");
        },
    )
}
// --8<-- [end:app]
