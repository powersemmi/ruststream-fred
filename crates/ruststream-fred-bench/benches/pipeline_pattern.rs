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
//! The Pub/Sub pattern scenario with a window, the pattern twin of `pipeline_pubsub`: the same
//! handler on a glob, fed on a channel it matches, plain and atomic.

mod common;

use std::hint::black_box;

use common::{Feed, Latch, MESSAGES, Order, Pending};
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream_fred::prelude::*;

// Twice MESSAGES deliveries allocated 54,664 blocks across repeated runs, the client's allocations
// as much as the crate's. The floor is the highest, stated over a thousand deliveries, plus a 0.1%
// margin of 55 blocks; one more allocation per delivery would add 2,000.
const FLOOR_PLAIN: u64 = 27_332;
const COLD_PLAIN: u64 = 55;
// Twice MESSAGES deliveries allocated 94,587 blocks across repeated runs, the client's allocations
// as much as the crate's. The floor is the highest, stated over a thousand deliveries, plus a 0.1%
// margin of 95 blocks; one more allocation per delivery would add 2,000.
const FLOOR_ATOMIC: u64 = 47_294;
const COLD_ATOMIC: u64 = 95;

#[subscriber(RedisPubSubPattern::new(common::PATTERN).buffer(common::PUBSUB_BUFFER))]
async fn consume_matched(
    order: &Order,
    ctx: &mut Context<'_, PipelineContext, Latch>,
) -> HandlerOutcome {
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
    common::pending_on(Feed::Pattern, messages, |b| {
        b.include(consume_matched.pipeline())
            .out_retry(pubsub::Publish::default())
            .to(common::PATTERN_RETRY);
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
    common::pending_on(Feed::Pattern, messages, |b| {
        b.include(consume_matched.pipeline().atomic())
            .out_retry(pubsub::Publish::default())
            .to(common::PATTERN_RETRY);
    })
}

#[library_benchmark(config = common::config_every(FLOOR_ATOMIC, 1_000, COLD_ATOMIC))]
#[bench::first(atomic_app(1))]
#[bench::base(atomic_app(MESSAGES))]
#[bench::twice(atomic_app(2 * MESSAGES))]
fn atomic(app: Pending) {
    common::start_and_drain(app);
}

library_benchmark_group!(name = pipeline_pattern_group; benchmarks = plain, atomic);
main!(library_benchmark_groups = pipeline_pattern_group);
