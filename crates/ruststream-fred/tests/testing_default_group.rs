//! The in-process broker takes the real broker's `default_group` setting and refuses a bare-name
//! stream subscription without it, as the real broker does: Redis Streams always read through a
//! consumer group, and a bare name names none.

#![cfg(feature = "testing")]

use ruststream::testing::TestApp;
use ruststream::{Broker, Subscribe};
use ruststream_fred::RedisError;
use ruststream_fred::stream::prelude::*;
use ruststream_fred::testing::RedisTestBroker;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Outgoing, PartialEq, Serialize)]
struct Order {
    id: u64,
}

#[subscriber("orders")]
async fn by_name(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bare_name_without_a_default_group_is_refused() {
    let connected = RedisTestBroker::new().connect().await.expect("connect");
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
    let connected = RedisTestBroker::new()
        .default_group("workers")
        .connect()
        .await
        .expect("connect");
    Subscribe::subscribe(&connected, "orders")
        .await
        .expect("a default group makes the bare name a subscription");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_service_mounting_a_bare_name_without_a_default_group_does_not_start() {
    let app =
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(RedisTestBroker::new(), |b| {
            b.include(by_name);
        });
    let err = TestApp::start(app)
        .await
        .expect_err("the service does not start");
    assert!(
        format!("{err:?}").contains("default_group"),
        "the startup error names the setting: {err:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_service_mounting_a_bare_name_with_a_default_group_delivers() {
    let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        RedisTestBroker::new().default_group("workers"),
        |b| {
            b.include(by_name);
        },
    );
    let tb = TestApp::start(app).await.expect("start");

    tb.broker::<RedisTestBroker>()
        .message(&Order { id: 1 })
        .to("orders")
        .publish()
        .await
        .expect("publish");
    tb.broker::<RedisTestBroker>()
        .subscriber("orders")
        .assert_called_once()
        .settled(HandlerOutcome::ack());

    tb.shutdown().await.expect("shutdown");
}
