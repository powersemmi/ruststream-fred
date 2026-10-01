//! The pipelined subscriptions as a transport: the conformance suites over every pipelined form,
//! and the cases whose subject is the subscription itself rather than an app on it.
//!
//! They live beside the code because a pipelined source has one public entry, the `.pipeline()`
//! mount step, and these cases open the subscription by hand. The live legs run against the
//! server behind `REDIS_TEST_URL`, and fail instead of skipping under `RUSTSTREAM_REQUIRE_LIVE`.

use std::pin::pin;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fred::interfaces::{KeysInterface, ListInterface, StreamsInterface};
use fred::types::{ClusterHash, CustomCommand};
use futures::{Stream, StreamExt};
use ruststream::conformance::harness::InProcessBroker;
use ruststream::conformance::{capabilities, harness};
use ruststream::testing::InProcess;
use ruststream::{
    Broker, BuildContext, ConnectedBroker, IncomingMessage, OutgoingMessage, Publisher, Subscriber,
    SubscriptionSource, nonzero,
};
use tokio::runtime::Builder;
use tokio::sync::oneshot;
use tokio::time::{Instant, timeout};

use super::{Atomic, Pipelined, Plain};
use crate::context::PipelineContext;
use crate::{
    ConnectedRedisBroker, RedisBroker, RedisError, RedisList, RedisListPublish, RedisPubSub,
    RedisPubSubPattern, RedisPubSubPublish, RedisStream,
};

/// The address the service's broker is built with; the in-process mode dials nothing.
const URL: &str = "redis://localhost:6379";

const WAIT: Duration = Duration::from_secs(10);

/// The variable a job sets to say it stood the servers up, so skipping past them is a defect.
const REQUIRE_LIVE: &str = "RUSTSTREAM_REQUIRE_LIVE";

/// The summary form of `XPENDING`: the count, the lowest and highest id, and the count per consumer.
type PendingSummary = (
    u64,
    Option<String>,
    Option<String>,
    Option<Vec<(String, String)>>,
);

/// The live server's URL, or `None` to skip; under [`REQUIRE_LIVE`] a missing URL fails.
fn redis_url() -> Option<String> {
    match std::env::var("REDIS_TEST_URL") {
        Ok(value) if !value.is_empty() => Some(value),
        _ => {
            assert!(
                !std::env::var(REQUIRE_LIVE).is_ok_and(|value| !value.is_empty()),
                "{REQUIRE_LIVE} is set, so this suite must run, but REDIS_TEST_URL is unset or empty",
            );
            None
        }
    }
}

/// A name unique to this run, so a live server that keeps an earlier run's keys answers nothing
/// of this one. The hash tag keeps a case's keys on one slot.
fn unique(base: &str) -> String {
    static RUN: OnceLock<u128> = OnceLock::new();
    static N: AtomicU64 = AtomicU64::new(0);
    let run = RUN.get_or_init(|| {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("a clock after the epoch")
            .as_nanos()
    });
    format!(
        "{{ut.pipeline.{base}.{run}.{}}}",
        N.fetch_add(1, Ordering::Relaxed)
    )
}

// The pipelined forms, as `.pipeline()` and `.atomic()` at a mount site make them.

fn stream(key: &str) -> Pipelined<RedisStream, Plain> {
    Pipelined::wrap(RedisStream::new(key).group("conformance"))
}

fn atomic_stream(key: &str) -> Pipelined<RedisStream, Atomic> {
    Pipelined::wrap(RedisStream::new(key).group("conformance"))
}

fn list(key: &str) -> Pipelined<RedisList, Plain> {
    Pipelined::wrap(RedisList::new(key).reliable())
}

fn atomic_list(key: &str) -> Pipelined<RedisList, Atomic> {
    Pipelined::wrap(RedisList::new(key).reliable())
}

/// Room for the lifecycle's burst: its message-shape step publishes 36 messages before it reads
/// one, more than `fred`'s default buffer of 32 holds.
fn pubsub(channel: &str) -> Pipelined<RedisPubSub, Plain> {
    Pipelined::wrap(RedisPubSub::new(channel).buffer(nonzero!(64)))
}

fn atomic_pubsub(channel: &str) -> Pipelined<RedisPubSub, Atomic> {
    Pipelined::wrap(RedisPubSub::new(channel).buffer(nonzero!(64)))
}

/// A pattern that matches the suite's channel and no other: the name holds no glob character.
fn pattern(channel: &str) -> Pipelined<RedisPubSubPattern, Plain> {
    Pipelined::wrap(RedisPubSubPattern::new(channel).buffer(nonzero!(64)))
}

fn atomic_pattern(channel: &str) -> Pipelined<RedisPubSubPattern, Atomic> {
    Pipelined::wrap(RedisPubSubPattern::new(channel).buffer(nonzero!(64)))
}

fn in_process() -> InProcessBroker<RedisBroker> {
    InProcessBroker::new(RedisBroker::standalone(URL))
}

fn stream_publisher(connected: &ConnectedRedisBroker) -> crate::RedisPublisher {
    connected.publisher()
}

fn list_publisher(connected: &ConnectedRedisBroker) -> crate::RedisListPublisher {
    connected.list_publisher(RedisListPublish::new())
}

fn pubsub_publisher(connected: &ConnectedRedisBroker) -> crate::RedisPubSubPublisher {
    connected.pubsub_publisher(RedisPubSubPublish::new())
}

/// Runs the lifecycle, the batching and, where the form reports one, the address promise over
/// one pipelined form, in process and against the live server.
macro_rules! conformance {
    ($in_process:ident, $live:ident, $source:expr, $publisher:expr) => {
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn $in_process() {
            Box::pin(harness::lifecycle(in_process, $source, $publisher)).await;
            Box::pin(capabilities::batches(in_process, $source, $publisher)).await;
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn $live() {
            let Some(url) = redis_url() else {
                return;
            };
            let broker = move || RedisBroker::standalone(url.clone());
            Box::pin(harness::lifecycle(broker.clone(), $source, $publisher)).await;
            Box::pin(capabilities::batches(broker, $source, $publisher)).await;
        }
    };
}

conformance!(
    in_process_stream_pipeline_conforms,
    stream_pipeline_conforms,
    stream,
    stream_publisher
);
conformance!(
    in_process_atomic_stream_conforms,
    atomic_stream_conforms,
    atomic_stream,
    stream_publisher
);
conformance!(
    in_process_list_pipeline_conforms,
    list_pipeline_conforms,
    list,
    list_publisher
);
conformance!(
    in_process_atomic_list_conforms,
    atomic_list_conforms,
    atomic_list,
    list_publisher
);
conformance!(
    in_process_pubsub_pipeline_conforms,
    pubsub_pipeline_conforms,
    pubsub,
    pubsub_publisher
);
conformance!(
    in_process_atomic_pubsub_conforms,
    atomic_pubsub_conforms,
    atomic_pubsub,
    pubsub_publisher
);
conformance!(
    in_process_pattern_pipeline_conforms,
    pattern_pipeline_conforms,
    pattern,
    pubsub_publisher
);
conformance!(
    in_process_atomic_pattern_conforms,
    atomic_pattern_conforms,
    atomic_pattern,
    pubsub_publisher
);

/// The address promise, on the forms that report one: a pattern names none, so it has no leg.
macro_rules! addressed {
    ($in_process:ident, $live:ident, $source:expr, $publisher:expr) => {
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn $in_process() {
            Box::pin(harness::redelivery_address(in_process, $source, $publisher)).await;
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn $live() {
            let Some(url) = redis_url() else {
                return;
            };
            Box::pin(harness::redelivery_address(
                move || RedisBroker::standalone(url.clone()),
                $source,
                $publisher,
            ))
            .await;
        }
    };
}

addressed!(
    in_process_stream_pipeline_addresses_its_copies,
    stream_pipeline_addresses_its_copies,
    stream,
    stream_publisher
);
addressed!(
    in_process_list_pipeline_addresses_its_copies,
    list_pipeline_addresses_its_copies,
    list,
    list_publisher
);
addressed!(
    in_process_pubsub_pipeline_addresses_its_copies,
    pubsub_pipeline_addresses_its_copies,
    pubsub,
    pubsub_publisher
);

// The window's own runtime: a flush a drop owes reaches the server wherever the drop happens.

/// Runs `work` on a current-thread runtime of its own on another thread, and stops that runtime
/// as soon as `work` returns: a task the work spawned there dies with it.
async fn on_foreign_runtime<Work, Fut, Output>(work: Work) -> Output
where
    Work: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = Output>,
    Output: Send + 'static,
{
    let (done, result) = oneshot::channel();
    thread::spawn(move || {
        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a current-thread runtime builds");
        let output = runtime.block_on(work());
        drop(runtime);
        let _ = done.send(output);
    });
    result.await.expect("the foreign runtime's work panicked")
}

/// Runs `work` on another thread with no runtime at all, the way a destructor runs when the last
/// owner is dropped outside any runtime.
async fn off_any_runtime(work: impl FnOnce() + Send + 'static) {
    let (done, finished) = oneshot::channel();
    thread::spawn(move || {
        work();
        let _ = done.send(());
    });
    finished
        .await
        .expect("the work outside any runtime panicked");
}

async fn connected_in_process() -> ConnectedRedisBroker {
    RedisBroker::standalone(URL)
        .connect_in_process()
        .await
        .expect("connect in process")
}

async fn next<S, M>(stream: &mut S) -> M
where
    S: Stream<Item = Result<M, RedisError>> + Unpin,
{
    timeout(WAIT, stream.next())
        .await
        .expect("a delivery within the wait")
        .expect("the stream is open")
        .expect("a delivery, not an error")
}

async fn publish_entries(broker: &ConnectedRedisBroker, key: &str, count: usize) {
    let publisher = broker.publisher();
    for _ in 0..count {
        publisher
            .publish(OutgoingMessage::new(key, b"entry".as_slice()), None)
            .await
            .expect("publish");
    }
}

/// Waits until the group owes exactly `expected` entries, and fails once the wait runs out.
async fn assert_pending(broker: &ConnectedRedisBroker, key: &str, expected: u64, why: &str) {
    let pool = broker.pool_handle().expect("live pool");
    let deadline = Instant::now() + WAIT;
    loop {
        let rows: Vec<(String, String, u64, u64)> = pool
            .xpending(key, "workers", (0_u64, "-", "+", 10_u64))
            .await
            .expect("xpending");
        if rows.len() as u64 == expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{why}: the group still owes {} entries, expected {expected}",
            rows.len()
        );
        tokio::task::yield_now().await;
    }
}

fn workers(key: &str) -> Pipelined<RedisStream, Plain> {
    Pipelined::wrap(RedisStream::new(key).group("workers"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_delivery_dropped_on_another_runtime_flushes_what_its_window_owes() {
    let broker = connected_in_process().await;
    let key = unique("abandon");
    let mut subscriber = workers(&key).subscribe(&broker).await.expect("subscribe");
    publish_entries(&broker, &key, 2).await;
    let (acked, dropped) = {
        let mut stream = pin!(subscriber.stream());
        (next(&mut stream).await, next(&mut stream).await)
    };

    // The ack waits in the window for the other delivery; dropping that one unsettled is what
    // sends the window, from a runtime that stops right after.
    on_foreign_runtime(async move || {
        acked.ack().await.expect("ack");
        drop(dropped);
    })
    .await;

    assert_pending(
        &broker,
        &key,
        1,
        "the window flush left on a runtime that stopped",
    )
    .await;
    drop(subscriber);
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_subscription_dropped_off_any_runtime_flushes_what_its_window_owes() {
    let broker = connected_in_process().await;
    let key = unique("stop");
    let mut subscriber = workers(&key).subscribe(&broker).await.expect("subscribe");
    publish_entries(&broker, &key, 2).await;
    let (acked, held) = {
        let mut stream = pin!(subscriber.stream());
        (next(&mut stream).await, next(&mut stream).await)
    };
    acked.ack().await.expect("ack");

    // The subscription stops while a delivery is still in hand: its drop owes the server the ack
    // the window holds, and it happens where no runtime is current.
    off_any_runtime(move || drop(subscriber)).await;

    assert_pending(&broker, &key, 1, "the stopping subscription sent nothing").await;
    drop(held);
    broker.shutdown().await.expect("shutdown");
}

// The window against a real server, where a refusal comes from Redis itself.

async fn connected(url: &str) -> ConnectedRedisBroker {
    RedisBroker::standalone(url)
        .connect()
        .await
        .expect("connect to redis")
}

/// A flush that fails is reported once, on the delivery stream, and names the subscription.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_flush_is_reported_once_on_the_delivery_stream() {
    let Some(url) = redis_url() else {
        return;
    };
    let stream = unique("failing");
    let watcher = connected(&url).await;
    let pool = watcher.pool_handle().expect("pool");
    // A list under the name the handler's command writes as a stream: the `XADD` fails with
    // `WRONGTYPE` when the window sends it.
    let wrong = format!("{stream}.wrong");
    let _: i64 = pool.lpush(wrong.as_str(), "x").await.expect("lpush");

    let mut subscription = SubscriptionSource::subscribe(workers(&stream), &watcher)
        .await
        .expect("subscribe");
    watcher
        .publisher()
        .publish(OutgoingMessage::new(stream.as_str(), b"{}"), None)
        .await
        .expect("publish");

    let mut deliveries = Box::pin(subscription.stream());
    let delivery = next(&mut deliveries).await;
    let pipeline = PipelineContext::build(&delivery).pipeline().clone();
    pipeline
        .xadd(
            wrong.as_str(),
            false,
            None::<()>,
            "*",
            vec![("field", "value")],
        )
        .await
        .expect("queued");
    delivery.ack().await.expect("the acknowledgement is taken");

    watcher
        .publisher()
        .publish(OutgoingMessage::new(stream.as_str(), b"{}"), None)
        .await
        .expect("publish");
    let reported = timeout(WAIT, deliveries.next())
        .await
        .expect("the stream answers")
        .expect("the stream goes on");
    let Err(RedisError::Flush(message)) = reported else {
        panic!("expected the failed flush, got {reported:?}");
    };
    assert!(
        message.contains(stream.as_str()) && message.contains("failed"),
        "the report names the subscription and the failure: {message}"
    );
    // Once: the next item is the next delivery.
    let next = timeout(WAIT, deliveries.next())
        .await
        .expect("the stream answers")
        .expect("the stream goes on");
    assert!(next.is_ok(), "the failure is reported once: {next:?}");

    drop(deliveries);
    drop(subscription);
    let _: i64 = pool
        .del(vec![stream.as_str(), wrong.as_str()])
        .await
        .expect("del");
    watcher.shutdown().await.expect("shutdown watcher");
}

/// Under `.atomic()` a segment runs whole or not at all: a command Redis refuses when it is queued
/// aborts the `EXEC`, so neither the handler's other commands nor the `XACK` run, and the entry
/// stays pending.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_atomic_segment_redis_refuses_runs_nothing() {
    let Some(url) = redis_url() else {
        return;
    };
    let stream = unique("refused");
    let audit = format!("{stream}.audit");
    let watcher = connected(&url).await;
    let pool = watcher.pool_handle().expect("pool");
    let source: Pipelined<RedisStream, Atomic> =
        Pipelined::wrap(RedisStream::new(stream.as_str()).group("workers"));
    let mut subscription = SubscriptionSource::subscribe(source, &watcher)
        .await
        .expect("subscribe");
    watcher
        .publisher()
        .publish(OutgoingMessage::new(stream.as_str(), b"{}"), None)
        .await
        .expect("publish");

    let mut deliveries = Box::pin(subscription.stream());
    let delivery = next(&mut deliveries).await;
    let pipeline = PipelineContext::build(&delivery).pipeline().clone();
    pipeline
        .lpush(audit.as_str(), "written")
        .await
        .expect("queued");
    // `INCR` takes one key: Redis refuses it inside `MULTI`, which aborts the `EXEC`.
    let incr = CustomCommand::new_static("INCR", ClusterHash::Random, false);
    pipeline
        .custom(incr, Vec::<String>::new())
        .await
        .expect("queued");
    delivery.ack().await.expect("the acknowledgement is taken");

    let written: i64 = pool.llen(audit.as_str()).await.expect("llen");
    assert_eq!(written, 0, "no command of the refused segment ran");
    let pending: PendingSummary = pool
        .xpending(stream.as_str(), "workers", ())
        .await
        .expect("xpending");
    assert_eq!(
        pending.0, 1,
        "the XACK inside the refused segment did not run"
    );

    drop(deliveries);
    drop(subscription);
    let _: i64 = pool
        .del(vec![stream.as_str(), audit.as_str()])
        .await
        .expect("del");
    watcher.shutdown().await.expect("shutdown watcher");
}
