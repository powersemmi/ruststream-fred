//! A pipelined Redis Streams service: the handler queues its own Redis commands into the
//! delivery's round, and they leave with the delivery's `XACK`, in one round trip with the rest of
//! the window, and only if the delivery is acknowledged.
//!
//! The audit entry is a slot publish bound to the round, and the receipt is a reply the `InRound`
//! transform puts there too. `AtomicStream` in place of `PipelinedStream` makes each delivery's
//! commands and its acknowledgement one `MULTI` / `EXEC`.
//!
//! Start a Redis server first (`docker run -p 6379:6379 redis:7`), then:
//!
//! ```text
//! cargo run --example fred_pipeline --features macros,json -- run
//! ```
//!
//! Publish an order from another terminal with the Redis CLI:
//!
//! ```text
//! redis-cli XADD orders '*' _payload '{"id":1}'
//! ```

// --8<-- [start:handler]
use ruststream_fred::stream::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

#[derive(Serialize, Outgoing)]
#[outgoing(name = "audit")]
struct Audit {
    id: u64,
}

#[derive(Serialize, Outgoing)]
#[outgoing(name = "receipts")]
struct Receipt {
    id: u64,
}

#[subscriber(PipelinedStream::new("orders").group("workers"), publish)]
async fn handle(
    order: &Order,
    Ctx(pipeline): Ctx<keys::Pipeline>,
    Out(out): Out<impl Bindable>,
) -> Result<Receipt, HandlerOutcome> {
    // A command queued into the round: it runs after the handler returns, with the `XACK`.
    if pipeline.hincrby("orders:count", "seen", 1).await.is_err() {
        return Err(HandlerOutcome::retry());
    }
    // A slot publish bound to the round leaves with it, and not without it.
    let out = pipeline.bind(out);
    if out
        .message(&Audit { id: order.id })
        .publish()
        .await
        .is_err()
    {
        return Err(HandlerOutcome::retry());
    }
    Ok(Receipt { id: order.id })
}
// --8<-- [end:handler]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            b.include(handle)
                .out_reply(Publish)
                .transform(InRound)
                .out(DefaultSlot, Publish)
                .build();
        },
    )
}
// --8<-- [end:app]
