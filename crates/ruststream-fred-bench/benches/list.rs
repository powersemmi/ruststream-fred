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
//! Consuming a small JSON body from a reliable list: a claim moves the entry to the processing
//! list, the dispatcher decodes it, the handler reads a field, and the ack removes it from the
//! processing list with an `LREM`.

mod common;

use std::hint::black_box;

use common::{Feed, Latch, MESSAGES, Order, Pending};
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream_fred::prelude::*;

// Twice MESSAGES deliveries allocated 52,391 blocks across repeated runs, the client's allocations
// as much as the crate's. The floor is the highest, stated over a thousand deliveries, plus a 0.1%
// margin of 53 blocks; one more allocation per delivery would add 2,000.
const FLOOR_SERVICE: u64 = 26_196;
const COLD_SERVICE: u64 = 53;

#[subscriber(RedisList::new(common::INPUT).reliable())]
async fn consume(order: &Order, ctx: &mut Context<'_, (), Latch>) -> HandlerOutcome {
    black_box((order.id, order.quantity));
    ctx.state().arrived();
    HandlerOutcome::ack()
}

fn service_app(messages: usize) -> Pending {
    common::pending_on(Feed::List, messages, |b| {
        b.include(consume);
    })
}

#[library_benchmark(config = common::config_every(FLOOR_SERVICE, 1_000, COLD_SERVICE))]
#[bench::first(service_app(1))]
#[bench::base(service_app(MESSAGES))]
#[bench::twice(service_app(2 * MESSAGES))]
fn service(app: Pending) {
    common::start_and_drain(app);
}

library_benchmark_group!(name = list_group; benchmarks = service);
main!(library_benchmark_groups = list_group);
