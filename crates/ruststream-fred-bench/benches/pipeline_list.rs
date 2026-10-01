// The harness macros generate the group module, its items and the paths between them, and a
// benchmark function takes its setup value by value because the harness owns the drop; the
// crate's lints are written for the library surface, not for generated benchmark scaffolding.
#![allow(
    missing_docs,
    unused_qualifications,
    unreachable_pub,
    clippy::must_use_candidate,
    clippy::needless_pass_by_value
)]
//! The reliable list scenario with a window: `.pipeline()` claims a fetch with one pipeline of
//! `LMOVE` and sends the `LREM` settles together, and `.pipeline().atomic()` wraps each delivery's
//! settle in its own `MULTI` / `EXEC`. The handler queues one `INCR` of its own into its segment.

mod common;

use std::hint::black_box;

use common::{Feed, Latch, MESSAGES, Order, Pending};
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream_fred::prelude::*;

// Twice MESSAGES deliveries allocated 98,975 blocks across repeated runs, the client's allocations
// as much as the crate's. The floor is the highest, stated over a thousand deliveries, plus a 0.1%
// margin of 99 blocks; one more allocation per delivery would add 2,000.
const FLOOR_PLAIN: u64 = 49_488;
const COLD_PLAIN: u64 = 99;
// Twice MESSAGES deliveries allocated 138,389 blocks across repeated runs, the client's allocations
// as much as the crate's. The floor is the highest, stated over a thousand deliveries, plus a 0.1%
// margin of 139 blocks; one more allocation per delivery would add 2,000.
const FLOOR_ATOMIC: u64 = 69_195;
const COLD_ATOMIC: u64 = 139;

#[subscriber(RedisList::new(common::INPUT).reliable())]
async fn consume(order: &Order, ctx: &mut Context<'_, PipelineContext, Latch>) -> HandlerOutcome {
    black_box((order.id, order.quantity));
    // One command of the handler's own, queued into the delivery's segment: what a window is for.
    if ctx
        .context(keys::Pipeline)
        .incr(common::SEEN)
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    ctx.state().arrived();
    HandlerOutcome::ack()
}

fn plain_app(messages: usize) -> Pending {
    common::pending_on(Feed::List, messages, |b| {
        b.include(consume.pipeline());
    })
}

#[library_benchmark(config = common::config_every(FLOOR_PLAIN, 1_000, COLD_PLAIN))]
#[bench::first(plain_app(1))]
#[bench::base(plain_app(MESSAGES))]
#[bench::twice(plain_app(2 * MESSAGES))]
fn plain(app: Pending) {
    common::start_and_drain(app);
}

fn atomic_app(messages: usize) -> Pending {
    common::pending_on(Feed::List, messages, |b| {
        b.include(consume.pipeline().atomic());
    })
}

#[library_benchmark(config = common::config_every(FLOOR_ATOMIC, 1_000, COLD_ATOMIC))]
#[bench::first(atomic_app(1))]
#[bench::base(atomic_app(MESSAGES))]
#[bench::twice(atomic_app(2 * MESSAGES))]
fn atomic(app: Pending) {
    common::start_and_drain(app);
}

library_benchmark_group!(name = pipeline_list_group; benchmarks = plain, atomic);
main!(library_benchmark_groups = pipeline_list_group);
