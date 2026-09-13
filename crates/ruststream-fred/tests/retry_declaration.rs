//! The retry declaration at the mount site, on each of this crate's transports.
//!
//! `max_attempts(n)` and `dead_letter(name)` replace the per-descriptor knobs the crate used to
//! carry, and they read the same on a stream, a list and a channel. What differs underneath is
//! where the count comes from: the claiming read mode reports Redis's own, and everything else
//! counts through the framework's retry-count header on the copies the runtime publishes.

#![cfg(feature = "testing")]

use std::time::Duration;

use ruststream::testing::TestApp;
use ruststream_fred::testing::RedisTestBroker;
use ruststream_fred::{RedisList, RedisPubSub, RedisStream};
use serde::{Deserialize, Serialize};

use ruststream::prelude::*;

/// How long a claiming subscription leaves a retried entry pending before claiming it back.
const MIN_IDLE: Duration = Duration::from_secs(30);

#[derive(Debug, Deserialize, Outgoing, PartialEq, Serialize)]
struct Order {
    id: u64,
}

/// A handler that never succeeds, so the cap is the only thing that ends the message.
#[subscriber(RedisStream::new("orders").group("workers"))]
async fn never_ready(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::retry()
}

/// The same on a claiming subscription, whose deliveries carry the server's own count.
#[subscriber(RedisStream::claiming("claimed", MIN_IDLE).group("workers"))]
async fn never_ready_claimed(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::retry()
}

#[subscriber(RedisList::new("jobs").reliable())]
async fn never_ready_job(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::retry()
}

#[subscriber(RedisPubSub::new("events"))]
async fn never_ready_event(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::retry()
}

/// A stream counts through the framework's header, so an immediate retry becomes a copy the
/// runtime publishes back to the key, and the third delivery is the last the cap allows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stream_stops_at_the_cap_and_hands_the_last_delivery_over() {
    let app =
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(RedisTestBroker::new(), |b| {
            b.include(never_ready)
                .max_attempts(nonzero!(3u32))
                .dead_letter("orders.dead");
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.broker::<RedisTestBroker>()
        .message(&Order { id: 1 })
        .to("orders")
        .publish()
        .await
        .expect("publish");

    tb.broker::<RedisTestBroker>()
        .subscriber("orders")
        .assert_called(3);
    tb.broker::<RedisTestBroker>()
        .published::<Order>("orders.dead")
        .assert_called_once()
        .with(&Order { id: 1 });

    tb.shutdown().await.expect("shutdown");
}

/// A claiming subscription retries through the pending entries list, so the copies stay in Redis
/// and the cap is read from the count the server reports. The wait between attempts is the
/// subscription's `min_idle`, which the harness moves.
#[tokio::test(start_paused = true)]
async fn a_claiming_subscription_caps_on_the_servers_own_count() {
    let app =
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(RedisTestBroker::new(), |b| {
            b.include(never_ready_claimed)
                .max_attempts(nonzero!(3u32))
                .dead_letter("claimed.dead");
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.broker::<RedisTestBroker>()
        .message(&Order { id: 2 })
        .to("claimed")
        .publish()
        .await
        .expect("publish");
    tb.broker::<RedisTestBroker>()
        .subscriber("claimed")
        .assert_called_once();

    // Nothing is republished in between: the entry waits out `min_idle` twice, in Redis.
    tb.advance(MIN_IDLE).await.expect("first claim back");
    tb.advance(MIN_IDLE).await.expect("second claim back");

    tb.broker::<RedisTestBroker>()
        .subscriber("claimed")
        .assert_called(3);
    tb.broker::<RedisTestBroker>()
        .published::<Order>("claimed.dead")
        .assert_called_once()
        .with(&Order { id: 2 });

    tb.shutdown().await.expect("shutdown");
}

/// A reliable list has no count of its own, so the declaration reads exactly as it does on a
/// stream and the copies go back to the list key.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reliable_list_stops_at_the_cap() {
    let app =
        RustStream::new(AppInfo::new("jobs", "0.1.0")).with_broker(RedisTestBroker::new(), |b| {
            b.include(never_ready_job)
                .max_attempts(nonzero!(2u32))
                .dead_letter("jobs.failed");
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.broker::<RedisTestBroker>()
        .message(&Order { id: 3 })
        .to("jobs")
        .publish()
        .await
        .expect("publish");

    tb.broker::<RedisTestBroker>()
        .subscriber("jobs")
        .assert_called(2);
    tb.broker::<RedisTestBroker>()
        .published::<Order>("jobs.failed")
        .assert_called_once()
        .with(&Order { id: 3 });

    tb.shutdown().await.expect("shutdown");
}

/// Pub/Sub cannot settle at all, so every retry is a copy from the start, and the cap ends it the
/// same way.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_channel_stops_at_the_cap() {
    let app =
        RustStream::new(AppInfo::new("events", "0.1.0")).with_broker(RedisTestBroker::new(), |b| {
            b.include(never_ready_event)
                .max_attempts(nonzero!(2u32))
                .dead_letter("events.dead");
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.broker::<RedisTestBroker>()
        .message(&Order { id: 4 })
        .to("events")
        .publish()
        .await
        .expect("publish");

    tb.broker::<RedisTestBroker>()
        .subscriber("events")
        .assert_called(2);
    tb.broker::<RedisTestBroker>()
        .published::<Order>("events.dead")
        .assert_called_once()
        .with(&Order { id: 4 });

    tb.shutdown().await.expect("shutdown");
}
