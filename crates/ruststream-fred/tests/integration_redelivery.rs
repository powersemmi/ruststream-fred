//! The runtime's redelivery on a real server, one case per subscription form: a delivery that
//! asks for `retry_after` comes back once through its own form's publish, and at the cap it moves
//! to the dead-letter destination the same way. No mount here names a retry publisher, so what is
//! tested is the one a registration gets by default.
//!
//! ```bash
//! just brokers-up
//! REDIS_TEST_URL=redis://127.0.0.1:6379 \
//!     cargo test -p ruststream-fred --all-features --test integration_redelivery
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::StreamExt;
use ruststream::runtime::RETRY_COUNT_HEADER;
use ruststream::{
    Broker, ConnectedBroker, IncomingMessage, OutgoingMessage, Publisher, Subscriber,
};
use ruststream_fred::prelude::*;
use ruststream_fred::{ConnectedRedisBroker, RedisListPublish, RedisPubSubPublish};
use serde::{Deserialize, Serialize};

mod live;

/// How long a copy waits before it is published back.
const DELAY: Duration = Duration::from_millis(200);

/// How long the dead-letter destination is watched before the case fails.
const WAIT: Duration = Duration::from_secs(10);

/// The body every case publishes, in the default codec's encoding.
const BODY: &[u8] = br#"{"id":7}"#;

#[derive(Debug, Deserialize, Serialize)]
struct Order {
    id: u64,
}

fn redis_url() -> Option<String> {
    live::url("REDIS_TEST_URL")
}

/// A name unique to this run and this base, the same on every call, so a descriptor written in a
/// `#[subscriber(..)]` attribute and the test body name one key.
fn key(base: &'static str) -> String {
    static RUN: OnceLock<u128> = OnceLock::new();
    static N: AtomicU64 = AtomicU64::new(0);
    static NAMES: OnceLock<Mutex<Vec<(&'static str, String)>>> = OnceLock::new();
    let run = RUN.get_or_init(|| {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("a clock after the epoch")
            .as_nanos()
    });
    let mut names = NAMES
        .get_or_init(Default::default)
        .lock()
        .expect("names lock");
    if let Some((_, name)) = names.iter().find(|(known, _)| *known == base) {
        return name.clone();
    }
    let name = format!(
        "ruststream-it.redelivery.{base}.{run}.{}",
        N.fetch_add(1, Ordering::Relaxed)
    );
    names.push((base, name.clone()));
    name
}

/// Never succeeds, so the delay and then the cap decide where the message goes.
#[subscriber(RedisStream::new(key("stream")).group("workers"))]
async fn stream_handler(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::retry_after(DELAY)
}

#[subscriber(RedisList::new(key("reliable")).reliable())]
async fn reliable_handler(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::retry_after(DELAY)
}

#[subscriber(RedisList::new(key("plain")))]
async fn plain_handler(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::retry_after(DELAY)
}

#[subscriber(RedisPubSub::new(key("channel")))]
async fn channel_handler(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::retry_after(DELAY)
}

async fn connected(url: &str) -> ConnectedRedisBroker {
    RedisBroker::standalone(url)
        .connect()
        .await
        .expect("connect to redis")
}

/// Waits for the one message the dead-letter destination receives and checks it is the original,
/// carried there after one deferred copy: two copies were published for it, so its retry count is
/// two.
async fn assert_dead_lettered<S: Subscriber>(subscription: &mut S) {
    let mut deliveries = Box::pin(subscription.stream());
    let delivered = tokio::time::timeout(WAIT, deliveries.next())
        .await
        .expect("the dead-letter destination received nothing: the copy was lost on the way")
        .expect("the dead-letter subscription ended")
        .unwrap_or_else(|_| panic!("the dead-letter subscription failed"));
    assert_eq!(delivered.payload(), BODY);
    assert_eq!(
        delivered.headers().get(RETRY_COUNT_HEADER),
        Some(b"2".as_slice())
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stream_copy_and_its_dead_letter_leave_through_xadd() {
    let Some(url) = redis_url() else {
        return;
    };
    let dead = key("stream.dead");
    let app = RustStream::new(AppInfo::new("redelivery", "0.1.0")).with_broker(
        RedisBroker::standalone(url.clone()),
        |b| {
            b.include(stream_handler)
                .max_attempts(nonzero!(2u32))
                .dead_letter(dead.clone());
        },
    );
    let watcher = connected(&url).await;
    let mut dead_letters = watcher
        .subscribe(
            RedisStream::new(dead.clone())
                .group("check")
                .start_id(StreamStart::Beginning),
        )
        .await
        .expect("watch the dead-letter stream");
    let running = app.start().await.expect("start");

    watcher
        .publisher()
        .publish(OutgoingMessage::new(key("stream").as_str(), BODY), None)
        .await
        .expect("publish");
    assert_dead_lettered(&mut dead_letters).await;

    running.shutdown().await.expect("shutdown");
    watcher.shutdown().await.expect("shutdown watcher");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reliable_list_copy_and_its_dead_letter_leave_through_lpush() {
    let Some(url) = redis_url() else {
        return;
    };
    let dead = key("reliable.dead");
    let app = RustStream::new(AppInfo::new("redelivery", "0.1.0")).with_broker(
        RedisBroker::standalone(url.clone()),
        |b| {
            b.include(reliable_handler)
                .max_attempts(nonzero!(2u32))
                .dead_letter(dead.clone());
        },
    );
    let watcher = connected(&url).await;
    let mut dead_letters = watcher
        .subscribe_list(RedisList::new(dead.clone()))
        .await
        .expect("watch the dead-letter list");
    let running = app.start().await.expect("start");

    watcher
        .list_publisher(RedisListPublish::new())
        .publish(OutgoingMessage::new(key("reliable").as_str(), BODY), None)
        .await
        .expect("publish");
    assert_dead_lettered(&mut dead_letters).await;

    running.shutdown().await.expect("shutdown");
    watcher.shutdown().await.expect("shutdown watcher");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_plain_list_copy_and_its_dead_letter_leave_through_lpush() {
    let Some(url) = redis_url() else {
        return;
    };
    let dead = key("plain.dead");
    let app = RustStream::new(AppInfo::new("redelivery", "0.1.0")).with_broker(
        RedisBroker::standalone(url.clone()),
        |b| {
            b.include(plain_handler)
                .max_attempts(nonzero!(2u32))
                .dead_letter(dead.clone());
        },
    );
    let watcher = connected(&url).await;
    let mut dead_letters = watcher
        .subscribe_list(RedisList::new(dead.clone()))
        .await
        .expect("watch the dead-letter list");
    let running = app.start().await.expect("start");

    watcher
        .list_publisher(RedisListPublish::new())
        .publish(OutgoingMessage::new(key("plain").as_str(), BODY), None)
        .await
        .expect("publish");
    assert_dead_lettered(&mut dead_letters).await;

    running.shutdown().await.expect("shutdown");
    watcher.shutdown().await.expect("shutdown watcher");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_channel_copy_and_its_dead_letter_leave_through_publish() {
    let Some(url) = redis_url() else {
        return;
    };
    let dead = key("channel.dead");
    let app = RustStream::new(AppInfo::new("redelivery", "0.1.0")).with_broker(
        RedisBroker::standalone(url.clone()),
        |b| {
            b.include(channel_handler)
                .max_attempts(nonzero!(2u32))
                .dead_letter(dead.clone());
        },
    );
    let watcher = connected(&url).await;
    let mut dead_letters = watcher
        .subscribe_pubsub(RedisPubSub::new(dead.clone()))
        .await
        .expect("watch the dead-letter channel");
    let running = app.start().await.expect("start");

    watcher
        .pubsub_publisher(RedisPubSubPublish::new())
        .publish(OutgoingMessage::new(key("channel").as_str(), BODY), None)
        .await
        .expect("publish");
    assert_dead_lettered(&mut dead_letters).await;

    running.shutdown().await.expect("shutdown");
    watcher.shutdown().await.expect("shutdown watcher");
}
