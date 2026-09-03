//! Repositioning a Redis consumer group: a `start_at(..)` clause opens a subscription at a chosen
//! point in the stream, and the delivery's own context carries the handle that moves the group's
//! cursor while the service runs.
//!
//! Both act on the consumer group's cursor, which every consumer of that group shares: a seek
//! replays (or skips) for the whole worker pool, not just for the handler that asked.
//!
//! ```text
//! cargo run --example fred_seek --features macros,json -- run
//! ```
//!
//! Feed it from another terminal (id 0 marks the poison region the worker skips past):
//!
//! ```text
//! redis-cli XADD orders '*' _payload '{"id":1}'
//! redis-cli XADD orders '*' _payload '{"id":0}'
//! ```

use ruststream_fred::stream::prelude::*;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

// --8<-- [start:start-at]
// The audit trail replays from the oldest retained entry on every start: the clause takes a
// position constructor, and the subscription is sought there before its first delivery. Because
// the cursor belongs to the group, this rewinds the `auditors` group as a whole.
#[subscriber(
    RedisStream::new("audit").group("auditors"),
    start_at(RedisGroupPosition::beginning())
)]
async fn replay(order: &Order) -> HandlerOutcome {
    println!("audit: replayed order {}", order.id);
    HandlerOutcome::ack()
}
// --8<-- [end:start-at]

// --8<-- [start:seek-param]
// The worker owns its group's cursor: on the producer's poison marker it skips the group forward
// to the tail instead of grinding through the bad region. Every consumer of `workers` resumes
// there, which is the point - the region is bad for all of them. The handle is a context field:
// the `SeekHandle` key reads it off the delivery's own context, so nothing is attached at the
// mount site.
#[subscriber(RedisStream::new("orders").group("workers"))]
async fn handle(order: &Order, Ctx(seeker): Ctx<keys::SeekHandle>) -> HandlerOutcome {
    if order.id == 0 {
        if seeker.seek(RedisGroupPosition::end()).await.is_err() {
            return HandlerOutcome::retry();
        }
        println!("orders: skipped the poison region");
        return HandlerOutcome::ack();
    }
    println!("orders: processed {}", order.id);
    HandlerOutcome::ack()
}
// --8<-- [end:seek-param]

// --8<-- [start:batch]
// A page repositions the same group one level up: the handle is subscription-scoped, so it rides
// the batch context, while the entry a page reacts to is read off the page's own elements.
#[subscriber(RedisStream::new("orders.bulk").group("bulk"))]
async fn handle_page(
    orders: &[Order],
    ctx: &mut Context<'_, StreamBatchContext>,
) -> HandlerOutcome {
    if orders.iter().any(|order| order.id == 0) {
        println!(
            "orders.bulk: rewinding group {}",
            ctx.context(keys::ConsumerGroup)
        );
        if ctx
            .context(keys::SeekHandle)
            .seek(RedisGroupPosition::beginning())
            .await
            .is_err()
        {
            return HandlerOutcome::retry();
        }
    }
    HandlerOutcome::ack()
}
// --8<-- [end:batch]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            b.include(handle);
            b.include(handle_page);
            b.include(replay);
        },
    )
}
// --8<-- [end:app]
