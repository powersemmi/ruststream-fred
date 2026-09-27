//! The contract suites a broker crate supplies its own input to: the retry address of the bare
//! name, the keyed order and per-message options of the crate's publishers, the credential scans,
//! the settlement meanings, the shutdown flush and the in-process transport held to the server.
//!
//! Each runs in process, through `InProcessBroker`, and live against the server behind
//! `REDIS_TEST_URL` (and `REDIS_84_TEST_URL` for the claiming read mode, which Redis 8.4 added).
//! The suites that compare the two transports connect to both and run where the live suite runs.

#![cfg(feature = "testing")]

use std::time::Duration;

use ruststream::Name;
use ruststream::conformance::harness::InProcessBroker;
use ruststream::conformance::helpers::unique_subject;
use ruststream::conformance::in_process::{self, Refusal};
use ruststream::conformance::message_shape::{self, OptionCases};
use ruststream::conformance::{capabilities, lifecycle, retry, settlement};
use ruststream::testing::Backlog;
use ruststream::{HeaderMap, IncomingMessage};
use ruststream_fred::{
    RedisBroker, RedisList, RedisListPublish, RedisPubSub, RedisPubSubPublish, RedisPublish,
    RedisPublishOptions, RedisStream,
};

mod live;

/// The address the service's broker is built with; the in-process mode dials nothing.
const URL: &str = "redis://localhost:6379";

/// The consumer group every stream subscription here reads through.
const GROUP: &str = "conformance";

/// How long a delivery nobody settled stays with its consumer before the claiming read takes it
/// back.
const MIN_IDLE: Duration = Duration::from_secs(1);

/// The production broker a service builds, run in process.
fn in_process() -> InProcessBroker<RedisBroker> {
    InProcessBroker::new(RedisBroker::standalone(URL).default_group(GROUP))
}

fn stream(key: &str) -> RedisStream {
    RedisStream::new(key).group(GROUP)
}

fn list(key: &str) -> RedisList {
    RedisList::new(key).reliable()
}

fn pubsub(channel: &str) -> RedisPubSub {
    RedisPubSub::new(channel)
}

/// The options that carry `key` as the message's partition key.
#[allow(
    clippy::unnecessary_wraps,
    reason = "the suite asks for the options a keyed publish carries, and a Redis key always has some"
)]
fn keyed(key: &[u8], _headers: &mut HeaderMap) -> Option<RedisPublishOptions> {
    Some(RedisPublishOptions {
        partition_key: Some(key.to_vec()),
        ..RedisPublishOptions::default()
    })
}

/// The key a delivery reports, the effect a partition key has on the wire.
fn reported_key<M: IncomingMessage>(msg: &M) -> Option<Vec<u8>> {
    msg.partition_key().map(<[u8]>::to_vec)
}

/// A publish with no key arrives unkeyed; a call's key wins and is gone again on the next publish.
fn key_cases() -> OptionCases<RedisPublishOptions, Option<Vec<u8>>> {
    OptionCases::new(None).overrides(
        RedisPublishOptions {
            partition_key: Some(b"tenant-a".to_vec()),
            ..RedisPublishOptions::default()
        },
        Some(b"tenant-a".to_vec()),
    )
}

fn redis_url() -> Option<String> {
    live::url("REDIS_TEST_URL")
}

fn redis_84_url() -> Option<String> {
    live::url("REDIS_84_TEST_URL")
}

// The bare name `#[subscriber("orders")]` subscribes with: a consumer group through the broker's
// default group, whose copies go back to the stream key.

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_addresses_bare_name_copies() {
    Box::pin(retry::redelivery_address(
        in_process,
        |name| Name::new(name.to_owned()),
        |connected| connected.publisher(),
    ))
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn addresses_bare_name_copies() {
    let Some(url) = redis_url() else {
        return;
    };
    Box::pin(retry::redelivery_address(
        || RedisBroker::standalone(url.clone()).default_group(GROUP),
        |name| Name::new(name.to_owned()),
        |connected| connected.publisher(),
    ))
    .await;
}

// A key is the partition key the publish options carry; every form reports it on delivery and
// keeps one key's messages in order.

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_keeps_keyed_order() {
    let subject = unique_subject("conformance.keyed");
    Box::pin(message_shape::keyed_order(
        in_process,
        &subject,
        stream,
        |c| c.publisher(),
        keyed,
    ))
    .await;
    let subject = unique_subject("conformance.keyed");
    Box::pin(message_shape::keyed_order(
        in_process,
        &subject,
        list,
        |c| c.list_publisher(RedisListPublish::new()),
        keyed,
    ))
    .await;
    let subject = unique_subject("conformance.keyed");
    Box::pin(message_shape::keyed_order(
        in_process,
        &subject,
        pubsub,
        |c| c.pubsub_publisher(RedisPubSubPublish::new()),
        keyed,
    ))
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn keeps_keyed_order() {
    let Some(url) = redis_url() else {
        return;
    };
    let broker = || RedisBroker::standalone(url.clone());
    let subject = unique_subject("conformance.keyed");
    Box::pin(message_shape::keyed_order(
        broker,
        &subject,
        stream,
        |c| c.publisher(),
        keyed,
    ))
    .await;
    let subject = unique_subject("conformance.keyed");
    Box::pin(message_shape::keyed_order(
        broker,
        &subject,
        list,
        |c| c.list_publisher(RedisListPublish::new()),
        keyed,
    ))
    .await;
    let subject = unique_subject("conformance.keyed");
    Box::pin(message_shape::keyed_order(
        broker,
        &subject,
        pubsub,
        |c| c.pubsub_publisher(RedisPubSubPublish::new()),
        keyed,
    ))
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_resolves_publish_options() {
    let subject = unique_subject("conformance.options");
    Box::pin(message_shape::publish_options(
        in_process,
        &subject,
        stream,
        RedisPublish,
        key_cases(),
        reported_key,
    ))
    .await;
    let subject = unique_subject("conformance.options");
    Box::pin(message_shape::publish_options(
        in_process,
        &subject,
        list,
        RedisListPublish::new(),
        key_cases(),
        reported_key,
    ))
    .await;
    let subject = unique_subject("conformance.options");
    Box::pin(message_shape::publish_options(
        in_process,
        &subject,
        pubsub,
        RedisPubSubPublish::new(),
        key_cases(),
        reported_key,
    ))
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resolves_publish_options() {
    let Some(url) = redis_url() else {
        return;
    };
    let broker = || RedisBroker::standalone(url.clone());
    let subject = unique_subject("conformance.options");
    Box::pin(message_shape::publish_options(
        broker,
        &subject,
        stream,
        RedisPublish,
        key_cases(),
        reported_key,
    ))
    .await;
    let subject = unique_subject("conformance.options");
    Box::pin(message_shape::publish_options(
        broker,
        &subject,
        list,
        RedisListPublish::new(),
        key_cases(),
        reported_key,
    ))
    .await;
    let subject = unique_subject("conformance.options");
    Box::pin(message_shape::publish_options(
        broker,
        &subject,
        pubsub,
        RedisPubSubPublish::new(),
        key_cases(),
        reported_key,
    ))
    .await;
}

// What reaches the generated document: no publish policy and no address carries a credential.

#[test]
fn publishes_without_credentials() {
    message_shape::publishes_without_credentials::<ruststream_fred::ConnectedRedisBroker, _>(
        &RedisPublish,
        "hunter2",
    );
    message_shape::publishes_without_credentials::<ruststream_fred::ConnectedRedisBroker, _>(
        &RedisListPublish::new(),
        "hunter2",
    );
    message_shape::publishes_without_credentials::<ruststream_fred::ConnectedRedisBroker, _>(
        &RedisPubSubPublish::new(),
        "hunter2",
    );
}

#[test]
fn describes_addresses_without_credentials() {
    message_shape::describes_addresses_without_credentials(
        |addresses| RedisBroker::cluster(addresses.iter().copied()),
        "redis",
    );
    message_shape::describes_addresses_without_credentials(
        |addresses| RedisBroker::sentinel("mymaster", addresses.iter().copied()),
        "redis",
    );
}

// Repositioning a batch subscription: the stream subscription is both.

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_passes_batch_seeking() {
    Box::pin(capabilities::batch_seeking(
        in_process,
        |key| stream(key).block(Duration::from_millis(50)),
        |connected| connected.publisher(),
    ))
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn passes_batch_seeking() {
    let Some(url) = redis_url() else {
        return;
    };
    Box::pin(capabilities::batch_seeking(
        || RedisBroker::standalone(url.clone()),
        |key| stream(key).block(Duration::from_millis(50)),
        |connected| connected.publisher(),
    ))
    .await;
}

// What a shutdown finishes. A consumer group and a list keep what reaches them for a reader that
// comes later; a channel keeps nothing. The in-process server lives with one connection, so the
// second connection this suite opens would reach another one: it runs live only.

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_flushes() {
    let Some(url) = redis_url() else {
        return;
    };
    let broker = || RedisBroker::standalone(url.clone());
    Box::pin(lifecycle::shutdown_flushes(
        broker,
        stream,
        |c| c.publisher(),
        Backlog::Delivered,
    ))
    .await;
    Box::pin(lifecycle::shutdown_flushes(
        broker,
        list,
        |c| c.list_publisher(RedisListPublish::new()),
        Backlog::Delivered,
    ))
    .await;
    Box::pin(lifecycle::shutdown_flushes(
        broker,
        pubsub,
        |c| c.pubsub_publisher(RedisPubSubPublish::new()),
        Backlog::Missed,
    ))
    .await;
}

// The settlement meanings, on the server and in process alike.

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn settlements_match_the_server() {
    let Some(url) = redis_84_url() else {
        return;
    };
    Box::pin(settlement::matches_in_process(
        || RedisBroker::standalone(url.clone()),
        |key| RedisStream::claiming(key, MIN_IDLE).group(GROUP),
        |c| c.publisher(),
        MIN_IDLE,
    ))
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_settlements_match_the_server() {
    let Some(url) = redis_url() else {
        return;
    };
    // Recovery on: without it an entry a consumer took and never settled stays on the
    // processing list, which is the documented price of leaving recovery off.
    Box::pin(settlement::matches_in_process(
        || RedisBroker::standalone(url.clone()),
        |key| {
            RedisList::new(key)
                .recovery_zset(format!("{key}.recovery"))
                .min_idle(MIN_IDLE)
                .block(Duration::from_millis(50))
        },
        |c| c.list_publisher(RedisListPublish::new()),
        MIN_IDLE,
    ))
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pubsub_settlements_match_the_server() {
    let Some(url) = redis_url() else {
        return;
    };
    Box::pin(settlement::matches_in_process(
        || RedisBroker::standalone(url.clone()),
        pubsub,
        |c| c.pubsub_publisher(RedisPubSubPublish::new()),
        Duration::ZERO,
    ))
    .await;
}

// The in-process transport held to the server: what a late subscription finds, and what both
// refuse.

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backlog_matches_the_server() {
    let Some(url) = redis_url() else {
        return;
    };
    Box::pin(in_process::backlog_matches_server(
        || RedisBroker::standalone(url.clone()).default_group(GROUP),
        |c| c.publisher(),
    ))
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refuses_like_the_server() {
    let Some(url) = redis_url() else {
        return;
    };
    Box::pin(in_process::refuses_like_the_server(
        || RedisBroker::standalone(url.clone()).default_group(GROUP),
        |c| c.publisher(),
        // A stream subscription reads through a consumer group, and one that names none is
        // refused before anything is sent.
        [Refusal::Subscription {
            source: RedisStream::new(unique_subject("conformance.refused")),
        }],
    ))
    .await;
}
