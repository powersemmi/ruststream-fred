//! What the generated `AsyncAPI` document says about a Redis service.
//!
//! The specification's `redis` binding has no fields, so everything this crate knows about a
//! channel travels in the `x-ruststream-redis` extension beside it. These cases are what holds
//! that body to its shape, and the credential scan is what keeps a password out of a document
//! that gets published.

#![cfg(all(feature = "asyncapi", feature = "testing"))]

use std::time::Duration;

use ruststream::SubscriptionSource;
use ruststream::asyncapi::build_spec;
use ruststream::conformance::harness;
use ruststream::prelude::*;
use ruststream_fred::testing::RedisTestBroker;
use ruststream_fred::{
    ConnectedRedisBroker, PubSubMode, RedisBroker, RedisList, RedisListPublish, RedisPubSub,
    RedisPubSubPattern, RedisPubSubPublish, RedisPublish, RedisStream,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// How long the claiming subscription below leaves a retried entry pending.
const MIN_IDLE: Duration = Duration::from_secs(30);

/// The expiry the list publisher below re-arms on its key.
const BATCH_TTL: Duration = Duration::from_secs(60);

#[derive(Debug, Deserialize, Serialize)]
struct Order {
    id: u64,
}

#[derive(Debug, Deserialize, Outgoing, Serialize)]
struct Receipt {
    id: u64,
}

#[subscriber(RedisStream::claiming("orders", MIN_IDLE).group("workers").consumer("worker-1"), publish("orders.done"))]
async fn handle_order(order: &Order) -> Receipt {
    Receipt { id: order.id }
}

#[subscriber(RedisList::new("jobs").reliable())]
async fn handle_job(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::ack()
}

#[subscriber(RedisPubSub::new("events").mode(PubSubMode::Sharded))]
async fn handle_event(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::ack()
}

#[subscriber(RedisStream::new("audit").group("workers"), publish("audit.done"))]
async fn handle_audit(order: &Order) -> Receipt {
    Receipt { id: order.id }
}

#[subscriber(RedisList::new("batches"), publish("batches.done"))]
async fn handle_batch(order: &Order) -> Receipt {
    Receipt { id: order.id }
}

/// The document a service on all three forms publishes.
fn document() -> Value {
    let app =
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(RedisTestBroker::new(), |b| {
            b.include(handle_order).out_reply(RedisPubSubPublish::new());
            b.include(handle_job);
            b.include(handle_event);
            b.include(handle_audit).out_reply(RedisPublish);
            b.include(handle_batch)
                .out_reply(RedisListPublish::new().ttl(BATCH_TTL));
        });
    let json = build_spec(&app).to_json().expect("the document serializes");
    serde_json::from_str(&json).expect("the document is JSON")
}

/// The extension body of the channel named `name`.
fn extension(document: &Value, name: &str) -> Value {
    document["channels"][name]["bindings"]["x-ruststream-redis"].clone()
}

/// A stream subscription reports how it reads: through which group and consumer, in which mode,
/// and from what idle threshold on.
#[test]
fn a_stream_channel_reports_its_group_and_read_mode() {
    // The excerpt the AsyncAPI page shows, so the page cannot drift from the document.
    let shown: Value = serde_json::from_str(include_str!("fixtures/asyncapi_stream_channel.json"))
        .expect("the page excerpt is JSON");
    assert_eq!(extension(&document(), "orders"), shown);
}

/// A list reports whether it acknowledges, where an unfinished entry sits while it does, and how
/// headers are framed beside the payload.
#[test]
fn a_list_channel_reports_its_reliability_and_framing() {
    assert_eq!(
        extension(&document(), "jobs"),
        serde_json::json!({
            "form": "list",
            "reliable": true,
            "processing": "jobs.processing",
            "envelope": { "contentType": "application/octet-stream" },
        }),
    );
}

/// A channel reports the delivery mode it is subscribed in, which decides whether a publisher
/// reaches it at all.
#[test]
fn a_pubsub_channel_reports_its_delivery_mode() {
    assert_eq!(
        extension(&document(), "events"),
        serde_json::json!({
            "form": "pubsub",
            "mode": "sharded",
            "pattern": false,
            "envelope": { "contentType": "application/octet-stream" },
        }),
    );
}

/// The reply channel is described by the policy that publishes to it, not by the subscription that
/// produced the reply.
#[test]
fn a_reply_channel_is_described_by_its_publisher() {
    assert_eq!(
        extension(&document(), "orders.done"),
        serde_json::json!({
            "form": "pubsub",
            "channel": "orders.done",
            "mode": "classic",
            "envelope": { "contentType": "application/octet-stream" },
        }),
    );
}

/// A publisher names where it lands in the word Redis uses for it, and the name is the destination
/// the mount site resolved rather than anything the policy carries: all three policies here are
/// the plain default value, and each channel reports its own name.
#[test]
fn a_publisher_names_the_destination_it_was_mounted_on() {
    let document = document();
    assert_eq!(
        extension(&document, "audit.done"),
        serde_json::json!({ "form": "stream", "key": "audit.done" }),
    );
    assert_eq!(
        extension(&document, "batches.done"),
        serde_json::json!({
            "form": "list",
            "key": "batches.done",
            "ttlMs": 60_000,
            "envelope": { "contentType": "application/octet-stream" },
        }),
    );
    assert_eq!(
        extension(&document, "orders.done")["channel"],
        "orders.done"
    );
}

/// A pattern subscription says it is one, so a reader knows the address is a glob rather than a
/// channel anything publishes to.
#[test]
fn a_pattern_channel_says_the_address_is_a_glob() {
    let bindings = SubscriptionSource::<ConnectedRedisBroker>::channel_bindings(
        &RedisPubSubPattern::new("events.*"),
    );
    let body: Value =
        serde_json::from_str(&serde_json::to_string(&bindings).expect("bindings serialize"))
            .expect("bindings are JSON");
    assert_eq!(
        body["x-ruststream-redis"],
        serde_json::json!({
            "form": "pubsub",
            "mode": "classic",
            "pattern": true,
            "envelope": { "contentType": "application/octet-stream" },
        }),
    );
}

/// The document is published and shared, so the password a broker was configured with must reach
/// neither the server description nor any binding body.
#[test]
fn the_document_carries_no_credential() {
    harness::describes_without_credentials(
        &RedisBroker::standalone("redis://user:hunter2@localhost:6379").password("hunter2"),
        &RedisStream::claiming("orders", MIN_IDLE)
            .group("workers")
            .consumer("worker-1"),
        "hunter2",
    );
}
