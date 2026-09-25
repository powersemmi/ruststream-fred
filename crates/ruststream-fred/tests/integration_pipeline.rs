//! The window of a `.pipeline()` subscription on a real server: what handlers queue and the
//! settles they owe reach Redis when the window flushes, and only for acknowledged deliveries.
//!
//! ```bash
//! just brokers-up
//! REDIS_TEST_URL=redis://127.0.0.1:6379 \
//!     cargo test -p ruststream-fred --all-features --test integration_pipeline
//! ```

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fred::interfaces::{ClientLike, KeysInterface, ListInterface, StreamsInterface};
use fred::types::{ClusterHash, CustomCommand};
use futures::StreamExt;
use ruststream::{
    Broker, BuildContext, ConnectedBroker, IncomingMessage, OutgoingMessage, Publisher, Subscriber,
    SubscriptionSource,
};
use ruststream_fred::context::{PipelineContext, keys};
use ruststream_fred::pipeline::{Bindable, InRound, RedisPipeline};
use ruststream_fred::prelude::*;
use ruststream_fred::{
    AtomicList, AtomicStream, ConnectedRedisBroker, DelayedRetry, PipelinedList, PipelinedPubSub,
    PipelinedStream, RedisError, RedisListPublish, RedisPubSubPublish,
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

#[subscriber(
    PipelinedStream::new(key("batch")).group("workers"),
    start_at(RedisGroupPosition::beginning())
)]
async fn batch(orders: &[Order], ctx: &mut Context<'_, PipelineContext>) -> HandlerOutcome {
    let pipeline = ctx.context(keys::Pipeline).clone();
    let audit = format!("{}.audit", key("batch"));
    for order in orders.iter().filter(|order| order.outcome != "drop") {
        if pipeline
            .lpush(audit.as_str(), order.id.to_string())
            .await
            .is_err()
        {
            return HandlerOutcome::retry();
        }
    }
    HandlerOutcome::ack()
}

/// Where the bound case's replies go; the hash tag keeps it on the stream's slot on a cluster.
const RECEIPTS: &str = "{it.pipeline.bound}.receipts";

#[derive(Serialize, Outgoing)]
struct Receipt {
    id: u64,
}

#[subscriber(
    AtomicStream::new(key("bound")).group("workers"),
    publish("{it.pipeline.bound}.receipts"),
    start_at(RedisGroupPosition::beginning())
)]
async fn bound(
    order: &Order,
    Ctx(pipeline): Ctx<keys::Pipeline>,
    Out(out): Out<impl Bindable>,
) -> Result<Receipt, HandlerOutcome> {
    let out = pipeline.bind(out);
    let audit = format!("{}.audit", key("bound"));
    if out
        .message(&Receipt { id: order.id })
        .to(audit.as_str())
        .publish()
        .await
        .is_err()
    {
        return Err(HandlerOutcome::retry());
    }
    if order.outcome == "drop" {
        return Err(HandlerOutcome::drop());
    }
    Ok(Receipt { id: order.id })
}

/// The same work as `queue_and_settle`, queued as a Lua script.
#[subscriber(
    PipelinedStream::new(key("script")).group("workers"),
    start_at(RedisGroupPosition::beginning())
)]
async fn scripted(order: &Order, Ctx(pipeline): Ctx<keys::Pipeline>) -> HandlerOutcome {
    let audit = format!("{}.audit", key("script"));
    let script = "return redis.call('LPUSH', KEYS[1], ARGV[1])";
    if pipeline
        .eval(script, vec![audit], vec![order.id.to_string()])
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    if order.outcome == "drop" {
        HandlerOutcome::drop()
    } else {
        HandlerOutcome::ack()
    }
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

#[subscriber(
    PipelinedStream::new(key("retry-refused"))
        .group("workers")
        .delayed_retry(DelayedRetry::DurableZset {
            key: format!("{}.delayed", key("retry-refused")),
            ttl: None,
        }),
    start_at(RedisGroupPosition::beginning())
)]
async fn retried(_order: &Order) -> HandlerOutcome {
    HandlerOutcome::retry_after(Duration::from_secs(60))
}

// A windowed retry whose schedule Redis refuses leaves its entry pending: the `XACK` goes out
// only once the write carrying the entry forward has succeeded.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retry_whose_write_fails_is_not_acknowledged() {
    let Some(url) = redis_url() else {
        return;
    };
    let stream = key("retry-refused");
    let delayed = format!("{stream}.delayed");
    let watcher = connected(&url).await;
    let pool = watcher.pool_handle().expect("pool");
    // The service connects as a user Redis refuses `ZADD`: every schedule onto the delay queue
    // fails, while reading, polling and acknowledging succeed.
    let user = format!("no-zadd-{}", stream.len());
    let _: fred::types::Value = pool
        .next()
        .custom(
            CustomCommand::new_static("ACL", ClusterHash::FirstKey, false),
            vec![
                "SETUSER",
                user.as_str(),
                "on",
                ">limited",
                "~*",
                "&*",
                "+@all",
                "-zadd",
            ],
        )
        .await
        .expect("acl setuser");
    let limited = url.replacen("redis://", &format!("redis://{user}:limited@"), 1);
    let app = RustStream::new(AppInfo::new("pipeline", "0.1.0")).with_broker(
        RedisBroker::standalone(limited),
        |b| {
            b.include(retried);
        },
    );
    let running = app.start().await.expect("start");
    let body = serde_json::to_vec(&Order {
        id: 1,
        outcome: "retry".to_owned(),
    })
    .expect("encode");
    watcher
        .publisher()
        .publish(OutgoingMessage::new(stream.as_str(), &body), None)
        .await
        .expect("publish");

    // The delivery is read and handled; its schedule fails, and the entry stays owed to the group.
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        let groups: Vec<HashMap<String, fred::types::Value>> = pool
            .xinfo_groups(stream.as_str())
            .await
            .expect("xinfo groups");
        let read = groups.iter().any(|group| {
            group
                .get("last-delivered-id")
                .and_then(fred::types::Value::as_str)
                .is_some_and(|id| id != "0-0")
        });
        if read {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the delivery was never read"
        );
        tokio::task::yield_now().await;
    }
    running.shutdown().await.expect("shutdown");
    let pending: PendingSummary = pool
        .xpending(stream.as_str(), "workers", ())
        .await
        .expect("xpending");
    assert_eq!(pending.0, 1, "the entry was acknowledged without its retry");
    let _: i64 = pool
        .del(vec![stream.as_str(), delayed.as_str()])
        .await
        .expect("del");
    let _: fred::types::Value = pool
        .next()
        .custom(
            CustomCommand::new_static("ACL", ClusterHash::FirstKey, false),
            vec!["DELUSER", user.as_str()],
        )
        .await
        .expect("acl deluser");
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

/// A batch is one segment: its body's commands and the settles of every entry leave together.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_batch_window_sends_what_the_batch_queued_and_settles_every_entry() {
    let Some(url) = redis_url() else {
        return;
    };
    let app = RustStream::new(AppInfo::new("pipeline", "0.1.0")).with_broker(
        RedisBroker::standalone(url.clone()),
        |b| {
            b.include(batch.batch(nonzero!(8)));
        },
    );
    run_window(&url, "batch", app).await;
}

/// A bound slot publish and a reply in the round leave with the acknowledged delivery's segment,
/// inside its `MULTI` / `EXEC`, and a dropped delivery's never leave.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bound_publishes_and_replies_in_the_round_follow_the_outcome() {
    let Some(url) = redis_url() else {
        return;
    };
    let stream = key("bound");
    let receipts = RECEIPTS.to_owned();
    let app = RustStream::new(AppInfo::new("pipeline", "0.1.0")).with_broker(
        RedisBroker::standalone(url.clone()),
        |b| {
            b.include(bound)
                .out_reply(stream::Publish)
                .transform(InRound)
                .out(DefaultSlot, stream::Publish)
                .build();
        },
    );
    let watcher = connected(&url).await;
    let pool = watcher.pool_handle().expect("pool");
    // The reply destination is named in the attribute, so it is the same on every run.
    let _: i64 = pool.del(receipts.as_str()).await.expect("del");
    let running = app.start().await.expect("start");
    let publisher = watcher.publisher();
    for id in 0..DELIVERIES {
        let outcome = if id % 3 == 0 { "drop" } else { "ack" };
        let body = serde_json::to_vec(&Order {
            id: id as u64,
            outcome: outcome.to_owned(),
        })
        .expect("encode");
        publisher
            .publish(OutgoingMessage::new(stream.as_str(), &body), None)
            .await
            .expect("publish");
    }
    let acknowledged = (0..DELIVERIES).filter(|id| id % 3 != 0).count() as u64;
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        let audited: u64 = pool
            .xlen(format!("{stream}.audit").as_str())
            .await
            .expect("xlen");
        let replied: u64 = pool.xlen(receipts.as_str()).await.expect("xlen");
        if audited == acknowledged && replied == acknowledged {
            break;
        }
        assert!(
            audited <= acknowledged,
            "a dropped delivery's bound publish left"
        );
        assert!(replied <= acknowledged, "a dropped delivery's reply left");
        assert!(
            tokio::time::Instant::now() < deadline,
            "{audited} audited and {replied} replied of {acknowledged}"
        );
        tokio::task::yield_now().await;
    }
    running.shutdown().await.expect("shutdown");
    let _: i64 = pool
        .del(vec![stream.clone(), receipts, format!("{stream}.audit")])
        .await
        .expect("del");
    watcher.shutdown().await.expect("shutdown watcher");
}

/// A flush that fails is reported once, on the delivery stream, and names the subscription.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_flush_is_reported_once_on_the_delivery_stream() {
    let Some(url) = redis_url() else {
        return;
    };
    let stream = key("failing");
    let watcher = connected(&url).await;
    let pool = watcher.pool_handle().expect("pool");
    // A list under the name the handler's command writes as a stream: the `XADD` fails with
    // `WRONGTYPE` when the window sends it.
    let wrong = format!("{stream}.wrong");
    let _: i64 = pool.lpush(wrong.as_str(), "x").await.expect("lpush");

    let mut subscription = SubscriptionSource::subscribe(
        PipelinedStream::new(stream.as_str()).group("workers"),
        &watcher,
    )
    .await
    .expect("subscribe");
    watcher
        .publisher()
        .publish(OutgoingMessage::new(stream.as_str(), b"{}"), None)
        .await
        .expect("publish");

    let mut deliveries = Box::pin(subscription.stream());
    let delivery = tokio::time::timeout(WAIT, deliveries.next())
        .await
        .expect("a delivery")
        .expect("the stream goes on")
        .expect("a delivery, not an error");
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
    let reported = tokio::time::timeout(WAIT, deliveries.next())
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
    let next = tokio::time::timeout(WAIT, deliveries.next())
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
    let stream = key("refused");
    let audit = format!("{stream}.audit");
    let watcher = connected(&url).await;
    let pool = watcher.pool_handle().expect("pool");
    let mut subscription = SubscriptionSource::subscribe(
        AtomicStream::new(stream.as_str()).group("workers"),
        &watcher,
    )
    .await
    .expect("subscribe");
    watcher
        .publisher()
        .publish(OutgoingMessage::new(stream.as_str(), b"{}"), None)
        .await
        .expect("publish");

    let mut deliveries = Box::pin(subscription.stream());
    let delivery = tokio::time::timeout(WAIT, deliveries.next())
        .await
        .expect("a delivery")
        .expect("the stream goes on")
        .expect("a delivery, not an error");
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_queued_script_runs_with_the_ack() {
    let Some(url) = redis_url() else {
        return;
    };
    let app = RustStream::new(AppInfo::new("pipeline", "0.1.0")).with_broker(
        RedisBroker::standalone(url.clone()),
        |b| {
            b.include(scripted);
        },
    );
    run_window(&url, "script", app).await;
}
