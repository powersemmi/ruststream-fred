//! Conformance suites for the Redis broker. `run_suite` proves routing against the in-process
//! `RedisTestBroker` (no server, runs everywhere); `lifecycle` and the capability suites prove the
//! broker contract against the real `RedisBroker` and are gated behind `REDIS_TEST_URL`.
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
use ruststream_fred::{RedisBroker, RedisStream};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redis_test_broker_passes_conformance_suite() {
    harness::run_suite(RedisTestBroker::new).await;
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

fn redis_url() -> Option<String> {
    std::env::var("REDIS_TEST_URL").ok()
}
