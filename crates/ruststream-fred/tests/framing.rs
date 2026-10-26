//! How a list value and a channel message carry headers, seen from a service: a value another
//! client wrote arrives whole, whatever its bytes look like, and a payload of the service's own
//! that resembles the frame reads back unchanged.

#![cfg(feature = "testing")]

use fred::interfaces::{ListInterface, PubsubInterface};
use futures::StreamExt;
use ruststream::runtime::{App, AppInfo, HandlerOutcome, RustStream};
use ruststream::testing::{InProcess, TestApp};
use ruststream::{
    ConnectedBroker, Deserialized, HeaderMap, IncomingMessage, Outgoing, Serialized, Subscriber,
    SubscriptionSource, subscriber,
};
use ruststream_fred::{RedisBroker, RedisList, RedisPubSub};
use tokio::time::{Duration, timeout};

const URL: &str = "redis://localhost:6379";
const WAIT: Duration = Duration::from_secs(1);

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

/// A value another client pushed or published arrives as the payload it is, with no headers, even
/// when its first bytes read as a frame of a release before the marker.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_foreign_value_that_begins_with_zeros_arrives_whole() {
    let broker = RedisBroker::standalone(URL)
        .connect_in_process()
        .await
        .expect("connect in process");
    let pool = broker.pool_handle().expect("pool");

    let mut list = SubscriptionSource::subscribe(RedisList::new("framing.foreign"), &broker)
        .await
        .expect("subscribe list");
    let mut channel = broker
        .subscribe_pubsub(RedisPubSub::new("framing.foreign.channel"))
        .await
        .expect("subscribe channel");
    let _: i64 = pool.lpush("framing.foreign", ZEROS).await.expect("lpush");
    let _: i64 = pool
        .next()
        .publish("framing.foreign.channel", ZEROS)
        .await
        .expect("publish");

    let popped = timeout(WAIT, Box::pin(list.stream()).next())
        .await
        .expect("a list delivery")
        .expect("stream has next")
        .expect("delivery ok");
    assert_eq!(popped.payload(), ZEROS);
    assert_eq!(popped.headers(), &HeaderMap::new());
    let heard = timeout(WAIT, Box::pin(channel.stream()).next())
        .await
        .expect("a channel delivery")
        .expect("stream has next")
        .expect("delivery ok");
    assert_eq!(heard.payload(), ZEROS);
    assert_eq!(heard.headers(), &HeaderMap::new());

    broker.shutdown().await.expect("shutdown");
}
