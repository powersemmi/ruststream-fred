//! The window of a `.pipeline()` subscription on a real server: what handlers queue and the
//! settles they owe reach Redis when the window flushes, and only for acknowledged deliveries.
//!
//! ```bash
//! just brokers-up
//! REDIS_TEST_URL=redis://127.0.0.1:6379 \
//!     cargo test -p ruststream-fred --all-features --test integration_pipeline
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fred::interfaces::{KeysInterface, ListInterface, StreamsInterface};
use ruststream::{Broker, ConnectedBroker, OutgoingMessage, Publisher};
use ruststream_fred::context::keys;
use ruststream_fred::pipeline::RedisPipeline;
use ruststream_fred::prelude::*;
use ruststream_fred::{
    AtomicList, AtomicStream, ConnectedRedisBroker, PipelinedList, PipelinedPubSub,
    PipelinedStream, RedisListPublish, RedisPubSubPublish,
};
use serde::{Deserialize, Serialize};

mod live;

const WAIT: Duration = Duration::from_secs(10);

/// The summary form of `XPENDING`: the count, the lowest and highest id, and the count per consumer.
type PendingSummary = (
    u64,
    Option<String>,
    Option<String>,
    Option<Vec<(String, String)>>,
);
const DELIVERIES: usize = 20;

#[derive(Debug, Deserialize, Serialize)]
struct Order {
    id: u64,
    outcome: String,
}

fn redis_url() -> Option<String> {
    live::url("REDIS_TEST_URL")
}

/// A name unique to this run and this base, the same on every call, so a descriptor in a
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
    // A hash tag keeps a case's keys on one slot, which is what an atomic segment needs.
    let name = format!(
        "{{it.pipeline.{base}.{run}.{}}}",
        N.fetch_add(1, Ordering::Relaxed)
    );
    names.push((base, name.clone()));
    name
}

/// Queues the order's id onto the audit list, then settles the way the order says.
async fn queue_and_settle(
    order: &Order,
    pipeline: &RedisPipeline,
    audit: String,
) -> HandlerOutcome {
    if pipeline.lpush(audit, order.id.to_string()).await.is_err() {
        return HandlerOutcome::retry();
    }
    if order.outcome == "drop" {
        HandlerOutcome::drop()
    } else {
        HandlerOutcome::ack()
    }
}

#[subscriber(
    PipelinedStream::new(key("plain")).group("workers"),
    start_at(RedisGroupPosition::beginning())
)]
async fn plain(order: &Order, Ctx(pipeline): Ctx<keys::Pipeline>) -> HandlerOutcome {
    queue_and_settle(order, &pipeline, format!("{}.audit", key("plain"))).await
}

#[subscriber(
    AtomicStream::new(key("atomic")).group("workers"),
    start_at(RedisGroupPosition::beginning())
)]
async fn atomic(order: &Order, Ctx(pipeline): Ctx<keys::Pipeline>) -> HandlerOutcome {
    queue_and_settle(order, &pipeline, format!("{}.audit", key("atomic"))).await
}

#[subscriber(PipelinedPubSub::new(key("channel")))]
async fn channel(order: &Order, Ctx(pipeline): Ctx<keys::Pipeline>) -> HandlerOutcome {
    queue_and_settle(order, &pipeline, format!("{}.audit", key("channel"))).await
}

#[subscriber(PipelinedList::new(key("reliable")).reliable())]
async fn reliable(order: &Order, Ctx(pipeline): Ctx<keys::Pipeline>) -> HandlerOutcome {
    queue_and_settle(order, &pipeline, format!("{}.audit", key("reliable"))).await
}

#[subscriber(AtomicList::new(key("atomic-list")).reliable())]
async fn atomic_list(order: &Order, Ctx(pipeline): Ctx<keys::Pipeline>) -> HandlerOutcome {
    queue_and_settle(order, &pipeline, format!("{}.audit", key("atomic-list"))).await
}

#[subscriber(PipelinedList::new(key("simple")))]
async fn simple(order: &Order, Ctx(pipeline): Ctx<keys::Pipeline>) -> HandlerOutcome {
    queue_and_settle(order, &pipeline, format!("{}.audit", key("simple"))).await
}

async fn connected(url: &str) -> ConnectedRedisBroker {
    RedisBroker::standalone(url)
        .connect()
        .await
        .expect("connect to redis")
}

/// Publishes `DELIVERIES` orders, every third one dropped, and waits for the audit list to hold
/// the acknowledged ones and, on a stream, for the group to owe nothing.
async fn run_window(url: &str, base: &'static str, app: RustStream) {
    let stream = key(base);
    let audit = format!("{stream}.audit");
    let running = app.start().await.expect("start");
    let watcher = connected(url).await;
    let pool = watcher.pool_handle().expect("pool");

    let stream_publisher = watcher.publisher();
    let channel_publisher = watcher.pubsub_publisher(RedisPubSubPublish::new());
    let list_publisher = watcher.list_publisher(RedisListPublish::new());
    let on_channel = base == "channel";
    let on_list = matches!(base, "reliable" | "atomic-list" | "simple");
    let mut acknowledged = 0;
    for id in 0..DELIVERIES {
        let outcome = if id % 3 == 0 { "drop" } else { "ack" };
        acknowledged += usize::from(outcome == "ack");
        let body = serde_json::to_vec(&Order {
            id: id as u64,
            outcome: outcome.to_owned(),
        })
        .expect("encode");
        if on_list {
            list_publisher
                .publish(OutgoingMessage::new(stream.as_str(), &body), None)
                .await
                .expect("publish");
        } else if on_channel {
            channel_publisher
                .publish(OutgoingMessage::new(stream.as_str(), &body), None)
                .await
                .expect("publish");
        } else {
            stream_publisher
                .publish(OutgoingMessage::new(stream.as_str(), &body), None)
                .await
                .expect("publish");
        }
    }

    // Each acknowledged delivery's `LPUSH` lands once, and no dropped one's does.
    let mut seen = Vec::new();
    for _ in 0..acknowledged {
        let popped: Option<(String, String)> = pool
            .next()
            .brpop(audit.as_str(), WAIT.as_secs_f64())
            .await
            .expect("brpop");
        seen.push(
            popped
                .expect("an acknowledged delivery's command reached Redis")
                .1,
        );
    }
    let rest: i64 = pool.llen(audit.as_str()).await.expect("llen");
    assert_eq!(rest, 0, "a dropped delivery's command reached Redis");
    assert!(
        seen.iter()
            .all(|id| id.parse::<usize>().expect("an id") % 3 != 0),
        "a dropped delivery's command reached Redis: {seen:?}"
    );

    // The settles ride the same flush: the processing list holds nothing once the window has
    // left, and neither does the queue.
    if on_list {
        let processing = format!("{stream}.processing");
        let deadline = tokio::time::Instant::now() + WAIT;
        loop {
            let claimed: i64 = pool.llen(processing.as_str()).await.expect("llen");
            let queued: i64 = pool.llen(stream.as_str()).await.expect("llen");
            if claimed == 0 && queued == 0 {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "{claimed} entries still claimed, {queued} still queued"
            );
            tokio::task::yield_now().await;
        }
        let _: i64 = pool.del(processing.as_str()).await.expect("del");
    }

    // The settles ride the same flush: the group owes nothing once the window has left.
    if !on_channel && !on_list {
        let deadline = tokio::time::Instant::now() + WAIT;
        loop {
            let pending: PendingSummary = pool
                .xpending(stream.as_str(), "workers", ())
                .await
                .expect("xpending");
            if pending.0 == 0 {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "{} entries still pending",
                pending.0
            );
            tokio::task::yield_now().await;
        }
    }

    running.shutdown().await.expect("shutdown");
    let _: i64 = pool
        .del(vec![stream.as_str(), audit.as_str()])
        .await
        .expect("del");
    watcher.shutdown().await.expect("shutdown watcher");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_window_sends_what_acknowledged_handlers_queued_with_their_xack() {
    let Some(url) = redis_url() else {
        return;
    };
    let app = RustStream::new(AppInfo::new("pipeline", "0.1.0")).with_broker(
        RedisBroker::standalone(url.clone()),
        |b| {
            b.include(plain);
        },
    );
    run_window(&url, "plain", app).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_atomic_window_sends_each_segment_inside_multi_exec() {
    let Some(url) = redis_url() else {
        return;
    };
    let app = RustStream::new(AppInfo::new("pipeline", "0.1.0")).with_broker(
        RedisBroker::standalone(url.clone()),
        |b| {
            b.include(atomic);
        },
    );
    run_window(&url, "atomic", app).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_channel_window_sends_what_acknowledged_handlers_queued() {
    let Some(url) = redis_url() else {
        return;
    };
    let app = RustStream::new(AppInfo::new("pipeline", "0.1.0")).with_broker(
        RedisBroker::standalone(url.clone()),
        |b| {
            b.include(channel);
        },
    );
    run_window(&url, "channel", app).await;
}

macro_rules! list_case {
    ($test:ident, $handler:ident, $base:literal) => {
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn $test() {
            let Some(url) = redis_url() else {
                return;
            };
            let app = RustStream::new(AppInfo::new("pipeline", "0.1.0")).with_broker(
                RedisBroker::standalone(url.clone()),
                |b| {
                    b.include($handler);
                },
            );
            run_window(&url, $base, app).await;
        }
    };
}

list_case!(
    a_reliable_list_window_claims_in_batches_and_settles_with_lrem,
    reliable,
    "reliable"
);
list_case!(
    an_atomic_reliable_list_window_settles_inside_each_segment,
    atomic_list,
    "atomic-list"
);
list_case!(a_simple_list_window_pops_in_batches, simple, "simple");
