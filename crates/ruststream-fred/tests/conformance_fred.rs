//! Conformance suites for the Redis broker. `run_suite` proves routing against the handler-stub
//! `RedisTestClient` (no server, runs everywhere); `lifecycle` and the batch capability prove the
//! lazy-startup contract and `BatchSubscriber` against the real `RedisBroker` and are gated behind
//! `REDIS_TEST_URL`.
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

use ruststream::conformance::{capabilities, harness};
use ruststream::testing::TestClient;
use ruststream_fred::testing::RedisTestClient;
use ruststream_fred::{RedisBroker, RedisStream};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redis_test_client_passes_conformance_suite() {
    harness::run_suite(RedisTestClient::start).await;
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
        |broker| broker.publisher(),
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
        |broker| broker.publisher(),
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn passes_transactions() {
    let Some(url) = redis_url() else {
        return;
    };
    capabilities::transactions(
        || RedisBroker::standalone(url.clone()),
        |key| RedisStream::new(key).group("conformance"),
        |broker| broker.publisher(),
    )
    .await;
}

fn redis_url() -> Option<String> {
    std::env::var("REDIS_TEST_URL").ok()
}
