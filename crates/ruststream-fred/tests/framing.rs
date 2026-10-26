//! How a list value and a channel message carry headers, seen from a service: a value another
//! client wrote arrives whole, whatever its bytes look like, and a payload of the service's own
//! that resembles the frame reads back unchanged.

#![cfg(feature = "testing")]

use std::collections::BTreeMap;

use fred::interfaces::{ListInterface, PubsubInterface};
use ruststream::runtime::{App, AppInfo, Ctx, HandlerOutcome, Headers, RustStream};
use ruststream::testing::TestApp;
use ruststream::{Deserialized, Outgoing, Serialized, subscriber};
use ruststream_fred::context::keys;
use ruststream_fred::{RedisBroker, RedisList, RedisPubSub};

const URL: &str = "redis://localhost:6379";

/// The bytes of a frame with no headers as a release before the marker wrote it, and of any value
/// that happens to begin with four zero bytes.
const ZEROS: &[u8] = &[0, 0, 0, 0, 0xab, 0xcd];

/// A payload that begins the way a frame of this crate begins: a count, then the marker header.
fn lookalike() -> Vec<u8> {
    let mut bytes = vec![0, 0, 0, 1, 0, 0, 0, 16];
    bytes.extend_from_slice(b"ruststream-frame");
    bytes.extend_from_slice(&[0, 0, 0, 0]);
    bytes.extend_from_slice(b"body");
    bytes
}

/// The delivery's bytes as they arrive.
#[derive(Deserialized)]
struct Raw<'a>(&'a [u8]);

/// Bytes that leave as they are.
#[derive(Outgoing, Serialized)]
struct Payload(Vec<u8>);

#[subscriber(RedisList::new("framing.jobs"))]
async fn job(raw: &Raw<'_>) -> HandlerOutcome {
    tracing::debug!(len = raw.0.len(), "job");
    HandlerOutcome::ack()
}

#[subscriber(RedisPubSub::new("framing.events"))]
async fn event(raw: &Raw<'_>) -> HandlerOutcome {
    tracing::debug!(len = raw.0.len(), "event");
    HandlerOutcome::ack()
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("framing", "0.1.0")).with_broker(
        RedisBroker::standalone(URL),
        |b| {
            b.include(job);
            b.include(event);
        },
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_payload_that_looks_like_the_frame_reaches_the_handler_unchanged() {
    let tb = TestApp::start(app()).await.expect("start");

    for name in ["framing.jobs", "framing.events"] {
        for bytes in [lookalike(), ZEROS.to_vec()] {
            tb.broker::<RedisBroker>()
                .message(&Payload(bytes.clone()))
                .to(name)
                .publish()
                .await
                .expect("publish");
            tb.broker::<RedisBroker>()
                .subscriber(name)
                .with_raw(&bytes)
                .settled(HandlerOutcome::ack());
        }
    }

    tb.shutdown().await.expect("shutdown");
}

/// Stands in for another client: on any trigger it writes the bare bytes to the list and the
/// channel with the pool's own `LPUSH` and `PUBLISH`, outside this crate's framing.
#[subscriber(RedisList::new("framing.foreign.trigger"))]
async fn foreign_producer(_trigger: &Raw<'_>, Ctx(pool): Ctx<keys::FredPool>) -> HandlerOutcome {
    let pushed: Result<i64, _> = pool.lpush("framing.foreign", ZEROS).await;
    let published: Result<i64, _> = pool.next().publish("framing.foreign.channel", ZEROS).await;
    if pushed.is_ok() && published.is_ok() {
        HandlerOutcome::ack()
    } else {
        HandlerOutcome::drop()
    }
}

/// Acks a delivery that carries no headers, and drops one that carries any.
fn bare(headers: &BTreeMap<String, String>) -> HandlerOutcome {
    if headers.is_empty() {
        HandlerOutcome::ack()
    } else {
        HandlerOutcome::drop()
    }
}

#[subscriber(RedisList::new("framing.foreign"))]
async fn foreign_job(
    _raw: &Raw<'_>,
    Headers(headers): Headers<BTreeMap<String, String>>,
) -> HandlerOutcome {
    bare(&headers)
}

#[subscriber(RedisPubSub::new("framing.foreign.channel"))]
async fn foreign_event(
    _raw: &Raw<'_>,
    Headers(headers): Headers<BTreeMap<String, String>>,
) -> HandlerOutcome {
    bare(&headers)
}

fn foreign_app() -> impl App {
    RustStream::new(AppInfo::new("framing-foreign", "0.1.0")).with_broker(
        RedisBroker::standalone(URL),
        |b| {
            b.include(foreign_producer);
            b.include(foreign_job);
            b.include(foreign_event);
        },
    )
}

/// A value another client pushed or published arrives as the payload it is, with no headers, even
/// when its first bytes read as a frame of a release before the marker.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_foreign_value_that_begins_with_zeros_arrives_whole() {
    let tb = TestApp::start(foreign_app()).await.expect("start");

    tb.broker::<RedisBroker>()
        .message(&Payload(b"go".to_vec()))
        .to("framing.foreign.trigger")
        .publish()
        .await
        .expect("publish");
    tb.settle().await.expect("settle");

    for name in ["framing.foreign", "framing.foreign.channel"] {
        tb.broker::<RedisBroker>()
            .subscriber(name)
            .assert_called_once()
            .with_raw(ZEROS)
            .settled(HandlerOutcome::ack());
    }

    tb.shutdown().await.expect("shutdown");
}
