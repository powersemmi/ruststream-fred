//! A `.pipeline()` stream subscription: what a handler queues through `Ctx<keys::Pipeline>` runs
//! after the handler, with the delivery's settle, and only when the delivery is acknowledged.
//!
//! The in-process broker runs the window as a server does: a handler's commands reach its
//! subscriptions when the window flushes, and every outcome but `ack` drops them.

#![cfg(feature = "testing")]

use std::time::Duration;

use ruststream::runtime::RETRY_COUNT_HEADER;
use ruststream::testing::TestApp;
use ruststream_fred::context::keys;
use ruststream_fred::prelude::*;
use ruststream_fred::{AtomicStream, PipelinedStream};
use serde::{Deserialize, Serialize};

/// The address the service's broker is built with; the in-process mode dials nothing.
const URL: &str = "redis://localhost:6379";

#[derive(Debug, Deserialize, Outgoing, PartialEq, Serialize)]
struct Order {
    id: u64,
    outcome: String,
}

fn order(id: u64, outcome: &str) -> Order {
    Order {
        id,
        outcome: outcome.to_owned(),
    }
}

/// Queues the order onto the audit list, then settles the way the order says.
async fn queue_and_settle(
    order: &Order,
    pipeline: &ruststream_fred::pipeline::RedisPipeline,
    retried: bool,
) -> HandlerOutcome {
    let body = serde_json::to_vec(order).expect("an order encodes");
    if pipeline.lpush("audit", body).await.is_err() {
        return HandlerOutcome::retry();
    }
    match order.outcome.as_str() {
        "drop" => HandlerOutcome::drop(),
        "retry" if !retried => HandlerOutcome::retry(),
        "later" if !retried => HandlerOutcome::retry_after(Duration::from_secs(30)),
        _ => HandlerOutcome::ack(),
    }
}

#[subscriber(PipelinedStream::new("orders").group("workers"))]
async fn windowed(order: &Order, ctx: &mut Context<'_, PipelineContext>) -> HandlerOutcome {
    let retried = ctx.headers().get(RETRY_COUNT_HEADER).is_some();
    let pipeline = ctx.context(keys::Pipeline).clone();
    queue_and_settle(order, &pipeline, retried).await
}

#[subscriber(AtomicStream::new("orders").group("workers"))]
async fn atomic(order: &Order, Ctx(pipeline): Ctx<keys::Pipeline>) -> HandlerOutcome {
    queue_and_settle(order, &pipeline, false).await
}

#[subscriber(PipelinedStream::new("orders").group("workers"))]
async fn queues_nothing(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::ack()
}

macro_rules! app_with {
    ($include:expr) => {
        RustStream::new(AppInfo::new("pipeline", "0.1.0"))
            .with_broker(RedisBroker::standalone(URL), $include)
    };
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_queued_command_leaves_with_the_ack() {
    let tb = TestApp::start(app_with!(|b| {
        b.include(windowed);
    }))
    .await
    .expect("start");

    tb.broker::<RedisBroker>()
        .message(&order(1, "ack"))
        .to("orders")
        .publish()
        .await
        .expect("publish");

    tb.broker::<RedisBroker>()
        .subscriber("orders")
        .assert_called_once()
        .settled(HandlerOutcome::ack());
    tb.broker::<RedisBroker>()
        .published::<Order>("audit")
        .assert_called_once()
        .with(&order(1, "ack"));

    tb.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dropped_delivery_leaves_nothing_it_queued() {
    let tb = TestApp::start(app_with!(|b| {
        b.include(windowed);
    }))
    .await
    .expect("start");

    tb.broker::<RedisBroker>()
        .message(&order(2, "drop"))
        .to("orders")
        .publish()
        .await
        .expect("publish");

    tb.broker::<RedisBroker>()
        .subscriber("orders")
        .assert_called_once()
        .settled(HandlerOutcome::drop());
    tb.broker::<RedisBroker>()
        .published::<Order>("audit")
        .assert_not_called();

    tb.shutdown().await.expect("shutdown");
}

/// A retry drops what the first delivery queued; the redelivered one acknowledges, and its own
/// command is the one that leaves.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retried_delivery_leaves_only_what_its_acknowledged_redelivery_queued() {
    let tb = TestApp::start(app_with!(|b| {
        b.include(windowed)
            .max_attempts(nonzero!(3u32))
            .dead_letter("orders.dead");
    }))
    .await
    .expect("start");

    tb.broker::<RedisBroker>()
        .message(&order(3, "retry"))
        .to("orders")
        .publish()
        .await
        .expect("publish");

    tb.broker::<RedisBroker>()
        .subscriber("orders")
        .assert_called(2);
    tb.broker::<RedisBroker>()
        .published::<Order>("audit")
        .assert_called_once()
        .with(&order(3, "retry"));

    tb.shutdown().await.expect("shutdown");
}

/// A delayed retry drops what the delivery queued too, and the copy comes back after the delay.
#[tokio::test(start_paused = true)]
async fn a_delayed_retry_leaves_nothing_it_queued() {
    let tb = TestApp::start(app_with!(|b| {
        b.include(windowed);
    }))
    .await
    .expect("start");

    tb.broker::<RedisBroker>()
        .message(&order(4, "later"))
        .to("orders")
        .publish()
        .await
        .expect("publish");
    tb.broker::<RedisBroker>()
        .published::<Order>("audit")
        .assert_not_called();

    tb.advance(Duration::from_secs(30)).await.expect("advance");
    tb.broker::<RedisBroker>()
        .subscriber("orders")
        .assert_called(2);
    tb.broker::<RedisBroker>()
        .published::<Order>("audit")
        .assert_called_once();

    tb.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_atomic_segment_leaves_with_the_ack() {
    let tb = TestApp::start(app_with!(|b| {
        b.include(atomic);
    }))
    .await
    .expect("start");

    tb.broker::<RedisBroker>()
        .message(&order(5, "ack"))
        .to("orders")
        .publish()
        .await
        .expect("publish");

    tb.broker::<RedisBroker>()
        .published::<Order>("audit")
        .assert_called_once()
        .with(&order(5, "ack"));

    tb.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_atomic_segment_of_a_dropped_delivery_never_leaves() {
    let tb = TestApp::start(app_with!(|b| {
        b.include(atomic);
    }))
    .await
    .expect("start");

    tb.broker::<RedisBroker>()
        .message(&order(6, "drop"))
        .to("orders")
        .publish()
        .await
        .expect("publish");

    tb.broker::<RedisBroker>()
        .subscriber("orders")
        .settled(HandlerOutcome::drop());
    tb.broker::<RedisBroker>()
        .published::<Order>("audit")
        .assert_not_called();

    tb.shutdown().await.expect("shutdown");
}

/// A window of several deliveries sends every acknowledged delivery's commands.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_acknowledged_delivery_of_a_window_leaves() {
    let tb = TestApp::start(app_with!(|b| {
        b.include(windowed);
    }))
    .await
    .expect("start");

    for id in 10..15 {
        tb.broker::<RedisBroker>()
            .message(&order(id, if id == 12 { "drop" } else { "ack" }))
            .to("orders")
            .publish()
            .await
            .expect("publish");
    }

    tb.broker::<RedisBroker>()
        .subscriber("orders")
        .assert_called(5);
    tb.broker::<RedisBroker>()
        .published::<Order>("audit")
        .assert_called(4);

    tb.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_delivery_that_queues_nothing_settles_as_before() {
    let tb = TestApp::start(app_with!(|b| {
        b.include(queues_nothing);
    }))
    .await
    .expect("start");

    tb.broker::<RedisBroker>()
        .message(&order(7, "ack"))
        .to("orders")
        .publish()
        .await
        .expect("publish");

    tb.broker::<RedisBroker>()
        .subscriber("orders")
        .assert_called_once()
        .settled(HandlerOutcome::ack());

    tb.shutdown().await.expect("shutdown");
}
