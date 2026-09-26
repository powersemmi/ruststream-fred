//! Conformance suites for the Redis broker, run twice: once in process, with the production
//! `RedisBroker` connected to its in-process server through `InProcessBroker`, and once against the
//! real server behind `REDIS_TEST_URL`.
//!
//! Both legs matter. The in-process leg applies the framework's own definition of correct broker
//! behaviour to the transport the test harness runs an app on, which is what keeps a passing
//! handler test meaningful; the live leg is what proves that transport is not lying about the
//! contract it reproduces.
//!
//! Run locally with a running Redis server:
//!
//! ```bash
//! just brokers-up
//! REDIS_TEST_URL=redis://127.0.0.1:6379 cargo test -p ruststream-fred --features testing --test conformance_fred
//! ```

#![cfg(feature = "testing")]

use std::time::Duration;

use ruststream::conformance::harness::InProcessBroker;
use ruststream::conformance::{capabilities, harness};
use ruststream_fred::{
    DelayedRetry, RedisBroker, RedisList, RedisListPublish, RedisPubSub, RedisPubSubPublish,
    RedisStream,
};

mod live;

/// The address the service's broker is built with; the in-process mode dials nothing.
const URL: &str = "redis://localhost:6379";

/// The production broker a service builds, run in process.
fn in_process() -> InProcessBroker<RedisBroker> {
    InProcessBroker::new(RedisBroker::standalone(URL))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_passes_conformance_suite() {
    // The suite subscribes by bare name, which reads through the broker-wide default group.
    harness::run_suite(|| RedisBroker::standalone(URL).default_group("conformance")).await;
}

// The in-process legs. Each names the descriptor and the publisher a service would, so the
// contract is applied to the wiring the crate ships rather than to a shape written for the suite.

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_passes_lifecycle() {
    Box::pin(harness::lifecycle(
        in_process,
        |key| RedisStream::new(key).group("conformance"),
        |connected| connected.publisher(),
    ))
    .await;
}

/// A stream with the durable delay queue, so the ladder's delayed nack takes the broker's own path
/// rather than the runtime's fallback. A short read block keeps the sweep that replays it prompt.
fn delayed(key: &str) -> RedisStream {
    RedisStream::new(key)
        .group("conformance")
        .delayed_retry(DelayedRetry::DurableZset {
            key: format!("{key}.delayed"),
            ttl: None,
        })
        .block(Duration::from_millis(50))
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_passes_delayed_retry_lifecycle() {
    Box::pin(harness::lifecycle(in_process, delayed, |connected| {
        connected.publisher()
    }))
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_passes_list_lifecycle() {
    harness::lifecycle(
        in_process,
        |key| RedisList::new(key).reliable(),
        |connected| connected.list_publisher(RedisListPublish::new()),
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_passes_pubsub_lifecycle() {
    harness::lifecycle(
        in_process,
        |channel| RedisPubSub::new(channel),
        |connected| connected.pubsub_publisher(RedisPubSubPublish::new()),
    )
    .await;
}

// What a descriptor addressing its own copies promises: publish to the address it reports and the
// subscription that reported it gets the message.

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_addresses_stream_copies() {
    Box::pin(harness::redelivery_address(
        in_process,
        |key| RedisStream::new(key).group("conformance"),
        |connected| connected.publisher(),
    ))
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_addresses_list_copies() {
    harness::redelivery_address(
        in_process,
        |key| RedisList::new(key).reliable(),
        |connected| connected.list_publisher(RedisListPublish::new()),
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_addresses_pubsub_copies() {
    harness::redelivery_address(
        in_process,
        |channel| RedisPubSub::new(channel),
        |connected| connected.pubsub_publisher(RedisPubSubPublish::new()),
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_passes_batches() {
    capabilities::batches(
        in_process,
        |key| RedisStream::new(key).group("conformance"),
        |connected| connected.publisher(),
    )
    .await;
}

// The two forms whose real deliveries cannot settle. The suite accepts an unsupported ack and
// rejects any other ack error, so these legs prove the batching contract holds on a subscription
// that refuses settlement.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_passes_pubsub_batches() {
    capabilities::batches(
        in_process,
        |channel| RedisPubSub::new(channel),
        |connected| connected.pubsub_publisher(RedisPubSubPublish::new()),
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_passes_list_batches() {
    capabilities::batches(
        in_process,
        |key| RedisList::new(key),
        |connected| connected.list_publisher(RedisListPublish::new()),
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_passes_transactions() {
    Box::pin(capabilities::transactions(
        in_process,
        |key| RedisStream::new(key).group("conformance"),
        |connected| connected.publisher(),
    ))
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_passes_owned_transactions() {
    capabilities::owned_transactions(
        in_process,
        |key| RedisStream::new(key).group("conformance"),
        |connected| connected.publisher(),
    )
    .await;
}

// The in-process server keeps a consumer group's cursor, so repositioning it is checked here as
// against a server.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_passes_seeking() {
    Box::pin(capabilities::seeking(
        in_process,
        |key| {
            RedisStream::new(key)
                .group("conformance")
                .block(Duration::from_millis(50))
        },
        |connected| connected.publisher(),
    ))
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn passes_lifecycle() {
    let Some(url) = redis_url() else {
        return;
    };
    // Boxed like the transaction suites: the live subscriber holds its own buffers, so the
    // ladder's future is too large to keep on the stack.
    Box::pin(harness::lifecycle(
        || RedisBroker::standalone(url.clone()),
        |key| RedisStream::new(key).group("conformance"),
        |connected| connected.publisher(),
    ))
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn passes_delayed_retry_lifecycle() {
    let Some(url) = redis_url() else {
        return;
    };
    Box::pin(harness::lifecycle(
        || RedisBroker::standalone(url.clone()),
        delayed,
        |connected| connected.publisher(),
    ))
    .await;
}

// The live halves of the two extra ladders.

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn passes_list_lifecycle() {
    let Some(url) = redis_url() else {
        return;
    };
    harness::lifecycle(
        || RedisBroker::standalone(url.clone()),
        |key| RedisList::new(key).reliable(),
        |connected| connected.list_publisher(RedisListPublish::new()),
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn passes_pubsub_lifecycle() {
    let Some(url) = redis_url() else {
        return;
    };
    harness::lifecycle(
        || RedisBroker::standalone(url.clone()),
        |channel| RedisPubSub::new(channel),
        |connected| connected.pubsub_publisher(RedisPubSubPublish::new()),
    )
    .await;
}

// The live halves of the address promise: a real `XADD` to the reported stream key, `LPUSH` to the
// reported list key and `PUBLISH` to the reported channel have to reach the subscription that
// named them.

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn addresses_stream_copies() {
    let Some(url) = redis_url() else {
        return;
    };
    Box::pin(harness::redelivery_address(
        || RedisBroker::standalone(url.clone()),
        |key| RedisStream::new(key).group("conformance"),
        |connected| connected.publisher(),
    ))
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn addresses_list_copies() {
    let Some(url) = redis_url() else {
        return;
    };
    harness::redelivery_address(
        || RedisBroker::standalone(url.clone()),
        |key| RedisList::new(key).reliable(),
        |connected| connected.list_publisher(RedisListPublish::new()),
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn addresses_pubsub_copies() {
    let Some(url) = redis_url() else {
        return;
    };
    harness::redelivery_address(
        || RedisBroker::standalone(url.clone()),
        |channel| RedisPubSub::new(channel),
        |connected| connected.pubsub_publisher(RedisPubSubPublish::new()),
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn passes_batches() {
    let Some(url) = redis_url() else {
        return;
    };
    capabilities::batches(
        || RedisBroker::standalone(url.clone()),
        |key| RedisStream::new(key).group("conformance"),
        |connected| connected.publisher(),
    )
    .await;
}

// The same suite over a transport with no native batches of its own: Pub/Sub assembles its
// batches on the client, and what it owes is what the stream owes - a batch never longer than the
// size the subscription opened with, and publish order preserved across batches.
//
// The list transport takes the same delegation and is checked the same way by
// `list_batches_are_capped_at_the_size_they_opened_with` in the integration tests instead: the suite
// names one fixed subject, and on Redis a list and a stream under one name are the same key.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn passes_pubsub_batches() {
    let Some(url) = redis_url() else {
        return;
    };
    capabilities::batches(
        || RedisBroker::standalone(url.clone()),
        |channel| RedisPubSub::new(channel),
        |connected| connected.pubsub_publisher(RedisPubSubPublish::new()),
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn passes_transactions() {
    let Some(url) = redis_url() else {
        return;
    };
    // Boxed for the same reason as the seeking suite below: the suite's future is large enough
    // that clippy rejects holding it on the stack.
    Box::pin(capabilities::transactions(
        || RedisBroker::standalone(url.clone()),
        |key| RedisStream::new(key).group("conformance"),
        |connected| connected.publisher(),
    ))
    .await;
}

// The owned transaction kind, next to the borrowed suite above. Both are gated on the standalone
// topology: cluster cannot offer multi-key transactions, so its publishers reject either kind.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn passes_owned_transactions() {
    let Some(url) = redis_url() else {
        return;
    };
    capabilities::owned_transactions(
        || RedisBroker::standalone(url.clone()),
        |key| RedisStream::new(key).group("conformance"),
        |connected| connected.publisher(),
    )
    .await;
}

// Repositioning is a single-key operation (`XGROUP SETID`), so it works on every topology; the
// suite runs on standalone like the others, and the cluster leg is covered by
// `cluster_seek_replays_history` in the integration tests, where the stream key's slot matters.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn passes_seeking() {
    let Some(url) = redis_url() else {
        return;
    };
    // Boxed: this suite drives the whole subscription state machine, and its future is large
    // enough that clippy rejects holding it on the stack.
    Box::pin(capabilities::seeking(
        || RedisBroker::standalone(url.clone()),
        |key| {
            // A short blocking read: the cursor moves immediately, but a subscription parked in
            // `XREADGROUP BLOCK` picks it up only on its next read.
            RedisStream::new(key)
                .group("conformance")
                .block(Duration::from_millis(50))
        },
        |connected| connected.publisher(),
    ))
    .await;
}

/// The server URL, or `None` to skip. Under `RUSTSTREAM_REQUIRE_LIVE` a missing variable fails the
/// test instead, so a job that started a server cannot report a suite that never ran.
fn redis_url() -> Option<String> {
    live::url("REDIS_TEST_URL")
}
