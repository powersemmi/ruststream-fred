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
//! Consuming a small JSON body from Pub/Sub, on a channel and on a pattern: the message arrives on
//! the subscription's own connection, the dispatcher decodes it, and the handler reads a field.
//! Pub/Sub settles nothing.

mod common;

use std::hint::black_box;

use common::{Feed, Latch, MESSAGES, Order, Pending};
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream_fred::prelude::*;

// Twice MESSAGES deliveries allocated 12,427 blocks across repeated runs, the client's allocations
// as much as the crate's. The floor is the highest, stated over a thousand deliveries, plus a 0.1%
// margin of 13 blocks; one more allocation per delivery would add 2,000.
const FLOOR_CHANNEL: u64 = 6_214;
const COLD_CHANNEL: u64 = 13;
// Twice MESSAGES deliveries allocated 12,452 blocks across repeated runs, the client's allocations
// as much as the crate's. The floor is the highest, stated over a thousand deliveries, plus a 0.1%
// margin of 13 blocks; one more allocation per delivery would add 2,000.
const FLOOR_PATTERN: u64 = 6_226;
const COLD_PATTERN: u64 = 13;

#[subscriber(RedisPubSub::new(common::INPUT).buffer(common::PUBSUB_BUFFER))]
async fn consume(order: &Order, ctx: &mut Context<'_, (), Latch>) -> HandlerOutcome {
    black_box((order.id, order.quantity));
    ctx.state().arrived();
    HandlerOutcome::ack()
}

#[subscriber(RedisPubSubPattern::new(common::PATTERN).buffer(common::PUBSUB_BUFFER))]
async fn consume_matched(order: &Order, ctx: &mut Context<'_, (), Latch>) -> HandlerOutcome {
    black_box((order.id, order.quantity));
    ctx.state().arrived();
    HandlerOutcome::ack()
}

fn channel_app(messages: usize) -> Pending {
    common::pending_on(Feed::Channel, messages, |b| {
        b.include(consume);
    })
}

#[library_benchmark(config = common::config_every(FLOOR_CHANNEL, 1_000, COLD_CHANNEL))]
#[bench::first(channel_app(1))]
#[bench::base(channel_app(MESSAGES))]
#[bench::twice(channel_app(2 * MESSAGES))]
fn channel(app: Pending) {
    common::start_and_drain(app);
}

fn pattern_app(messages: usize) -> Pending {
    common::pending_on(Feed::Pattern, messages, |b| {
        b.include(consume_matched)
            .out_retry(pubsub::Publish::default())
            .to(common::PATTERN_RETRY);
    })
}

#[library_benchmark(config = common::config_every(FLOOR_PATTERN, 1_000, COLD_PATTERN))]
#[bench::first(pattern_app(1))]
#[bench::base(pattern_app(MESSAGES))]
#[bench::twice(pattern_app(2 * MESSAGES))]
fn pattern(app: Pending) {
    common::start_and_drain(app);
}

library_benchmark_group!(name = pubsub_group; benchmarks = channel, pattern);
main!(library_benchmark_groups = pubsub_group);
