//! The in-process mode reads the production broker's `default_group` setting and refuses a
//! bare-name stream subscription without it, as the real broker does: Redis Streams always read
//! through a consumer group, and a bare name names none.

#![cfg(feature = "testing")]

use ruststream::Subscribe;
use ruststream::testing::{InProcess, TestApp};
use ruststream_fred::RedisError;
use ruststream_fred::stream::prelude::*;
use serde::{Deserialize, Serialize};

/// The address the service's broker is built with; the in-process mode dials nothing.
const URL: &str = "redis://localhost:6379";

#[derive(Debug, Deserialize, Outgoing, PartialEq, Serialize)]
struct Order {
    id: u64,
}

#[subscriber("orders")]
async fn by_name(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::ack()
}

/// The service's app, on the broker `main` builds.
fn app(broker: RedisBroker) -> RustStream {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(by_name);
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bare_name_without_a_default_group_is_refused() {
    let connected = RedisBroker::standalone(URL)
        .connect_in_process()
        .await
        .expect("connect");
    let err = Subscribe::subscribe(&connected, "orders")
        .await
        .expect_err("a bare name names no consumer group");
    let RedisError::InvalidOptions(message) = err else {
        panic!("expected InvalidOptions, got {err:?}");
    };
    assert!(
        message.contains("default_group") && message.contains("`orders`"),
        "the refusal names the setting and the key: {message}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bare_name_with_a_default_group_opens() {
    let connected = RedisBroker::standalone(URL)
        .default_group("workers")
        .connect_in_process()
        .await
        .expect("connect");
    Subscribe::subscribe(&connected, "orders")
        .await
        .expect("a default group makes the bare name a subscription");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_service_mounting_a_bare_name_without_a_default_group_does_not_start() {
    let err = TestApp::start(app(RedisBroker::standalone(URL)))
        .await
        .expect_err("the service does not start");
    assert!(
        format!("{err:?}").contains("default_group"),
        "the startup error names the setting: {err:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_service_mounting_a_bare_name_with_a_default_group_delivers() {
    let tb = TestApp::start(app(RedisBroker::standalone(URL).default_group("workers")))
        .await
        .expect("start");

    tb.broker::<RedisBroker>()
        .message(&Order { id: 1 })
        .to("orders")
        .publish()
        .await
        .expect("publish");
    tb.broker::<RedisBroker>()
        .subscriber("orders")
        .assert_called_once()
        .with(&Order { id: 1 })
        .settled(HandlerOutcome::ack());

    tb.shutdown().await.expect("shutdown");
}
