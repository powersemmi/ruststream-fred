//! Conformance suites for the Redis broker, run twice wherever the transport allows it: once
//! against the in-process `RedisTestBroker`, which needs no server and runs everywhere, and once
//! against the real `RedisBroker` behind `REDIS_TEST_URL`.
//!
//! Both legs matter. The in-process leg applies the framework's own definition of correct broker
//! behaviour to the stand-in, which is what keeps a passing handler test meaningful; the live leg
//! is what proves the stand-in is not lying about the contract it reproduces.
//!
//! One suite is live-only, and the reason is written above `passes_seeking`.
//!
//! Run locally with a running Redis server:
//!
//! ```bash
//! just brokers-up
//! REDIS_TEST_URL=redis://127.0.0.1:6379 cargo test -p ruststream-fred --features testing --test conformance_fred
//! ```
//!
//! In CI, the `broker-integration` job provides a Redis service first.

#![cfg(feature = "testing")]

use std::time::Duration;

use ruststream::conformance::{capabilities, harness};
use ruststream_fred::testing::RedisTestBroker;
use ruststream_fred::{RedisBroker, RedisList, RedisPubSub, RedisPubSubPublish, RedisStream};

mod live;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redis_test_broker_passes_conformance_suite() {
    harness::run_suite(RedisTestBroker::new).await;
}

// The in-process legs. Each names the descriptor and the publisher a service would, so the
// contract is applied to the wiring the crate ships rather than to a shape written for the suite.

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_broker_passes_lifecycle() {
    harness::lifecycle(
        RedisTestBroker::new,
        |key| RedisStream::new(key).group("conformance"),
        |connected| connected.publisher(),
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_broker_passes_batches() {
    capabilities::batches(
        RedisTestBroker::new,
        |key| RedisStream::new(key).group("conformance"),
        |connected| connected.publisher(),
    )
    .await;
}

// The two forms whose real deliveries cannot settle. The suite accepts an unsupported ack and
// rejects any other ack error, so these legs prove the batching contract holds on a subscription
// that refuses settlement; that the refusal happens at all is asserted in `testing_core`.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_broker_passes_pubsub_batches() {
    capabilities::batches(
        RedisTestBroker::new,
        |channel| RedisPubSub::new(channel),
        |connected| connected.plain_publisher(),
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_broker_passes_list_batches() {
    capabilities::batches(
        RedisTestBroker::new,
        |key| RedisList::new(key),
        |connected| connected.plain_publisher(),
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_broker_passes_transactions() {
    Box::pin(capabilities::transactions(
        RedisTestBroker::new,
        |key| RedisStream::new(key).group("conformance"),
        |connected| connected.publisher(),
    ))
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_broker_passes_owned_transactions() {
    capabilities::owned_transactions(
        RedisTestBroker::new,
        |key| RedisStream::new(key).group("conformance"),
        |connected| connected.publisher(),
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn passes_lifecycle() {
    let Some(url) = redis_url() else {
        return;
    };
    harness::lifecycle(
        || RedisBroker::standalone(url.clone()),
        |key| RedisStream::new(key).group("conformance"),
        |connected| connected.publisher(),
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
//
// This is the one suite with no in-process leg, and the reason is a capability the stand-in does
// not claim rather than an assertion it fails: `seeking` requires `Src::Subscriber: Seekable` with
// positions that round-trip through `Positioned`, and `RedisTestSubscriber` implements neither, so
// the leg would not compile. Claiming it would mean giving the stand-in a replayable cursor over
// its per-key log - a consumer-group cursor in all but name, and the one Redis behaviour the
// stand-in states outright that it does not simulate. Seeking is proven here against a real server
// and, on the cluster topology, in the integration tests.
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
