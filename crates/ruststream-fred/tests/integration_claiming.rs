//! Live tests for the claiming read mode, against the two servers the compose stand runs.
//!
//! `XREADGROUP ... CLAIM` arrived in Redis 8.4, so the mode has a server of its own
//! (`REDIS_84_TEST_URL`) and the older standalone (`REDIS_TEST_URL`) is where the startup refusal
//! is checked. Each is gated behind its variable, so a plain `cargo test` with no stand is a no-op.
//!
//! ```bash
//! just test-brokers
//! ```
//!
//! These cover what the in-process twins in `tests/claiming.rs` cannot: the wire shape of the
//! reply, the order the server returns claimed and fresh entries in, the counters coming from the
//! server's own pending entries list, and the refusal itself.

use std::slice;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fred::interfaces::StreamsInterface;
use futures::StreamExt;
use ruststream::{
    Broker, ConnectedBroker, IncomingMessage, OutgoingMessage, Publisher, Subscriber,
};
use ruststream_fred::{
    ConnectedRedisBroker, DELIVERY_COUNT_HEADER, IDLE_MS_HEADER, RedisBroker, RedisError,
    RedisStream,
};

mod live;

const WAIT: Duration = Duration::from_secs(5);

/// A short read block, so a read that finds nothing returns instead of sleeping the 5s default.
const BLOCK: Duration = Duration::from_millis(50);

/// Claims whatever is pending, whenever it was delivered. A service sets this above its longest
/// handler runtime; a test wants the claim to happen on the next read and not one clock tick
/// later, which is what makes these cases deterministic without sleeping.
const CLAIM_EVERYTHING: Duration = Duration::ZERO;

/// The URL of one server, or `None` to skip. Under `RUSTSTREAM_REQUIRE_LIVE` a missing variable
/// fails the test instead, so a job that started the stand cannot report a suite that never ran.
fn env(key: &str) -> Option<String> {
    live::url(key)
}

/// A stream key unique to this run, so a server kept between runs never hands one case the
/// pending entries of another. The counters these cases assert on are the group's own history, so
/// a key reused across runs would carry a delivery count from the last one.
fn unique_key(base: &str) -> String {
    static RUN: OnceLock<u128> = OnceLock::new();
    static N: AtomicU64 = AtomicU64::new(0);
    let run = RUN.get_or_init(|| {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("a clock after the epoch")
            .as_nanos()
    });
    format!(
        "ruststream-claim.{base}.{run}.{}",
        N.fetch_add(1, Ordering::Relaxed)
    )
}

async fn standalone(url: String) -> ConnectedRedisBroker {
    RedisBroker::standalone(url)
        .connect()
        .await
        .expect("connect to redis")
}

async fn next<S>(stream: &mut S) -> S::Item
where
    S: futures::Stream + Unpin,
{
    tokio::time::timeout(WAIT, stream.next())
        .await
        .expect("delivery within timeout")
        .expect("stream has a next item")
}

async fn publish(broker: &ConnectedRedisBroker, key: &str, body: &[u8]) {
    broker
        .publisher()
        .publish(OutgoingMessage::new(key, body), None)
        .await
        .expect("publish");
}

/// What the server's pending entries list says about the one entry it holds for `group`.
async fn pending(broker: &ConnectedRedisBroker, key: &str, group: &str) -> Vec<(String, u64, u64)> {
    let pool = broker.pool_handle().expect("a live pool");
    let rows: Vec<(String, String, u64, u64)> = pool
        .xpending(key, group, (0_u64, "-", "+", 10_u64))
        .await
        .expect("xpending");
    rows.into_iter()
        .map(|(id, _consumer, idle, count)| (id, idle, count))
        .collect()
}

/// Reads one entry through a fresh-tail consumer and drops it unsettled, the way a worker that
/// died mid-handler leaves it: in the group's pending entries list, idle from now on.
async fn orphan(broker: &ConnectedRedisBroker, key: &str, body: &[u8]) {
    let mut worker = broker
        .subscribe(RedisStream::new(key).group("workers").consumer("dead"))
        .await
        .expect("subscribe worker");
    publish(broker, key, body).await;
    {
        let mut stream = Box::pin(worker.stream());
        let msg = next(&mut stream).await.expect("worker delivery");
        assert_eq!(msg.payload(), body);
        drop(msg);
    }
    drop(worker);
}

/// One read brings back both sets, the stale one first. That ordering is the whole point of the
/// mode: a service that would otherwise need a second subscription gets recovery in the same
/// handler, ahead of the new work.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_read_returns_the_pending_entry_before_the_fresh_one() {
    let Some(url) = env("REDIS_84_TEST_URL") else {
        return;
    };
    let broker = standalone(url).await;
    let key = unique_key("order");

    orphan(&broker, &key, b"orphan").await;
    publish(&broker, &key, b"fresh").await;

    let mut sub = broker
        .subscribe(
            RedisStream::claiming(&key, CLAIM_EVERYTHING)
                .group("workers")
                .consumer("solo")
                .block(BLOCK),
        )
        .await
        .expect("subscribe claiming");
    let mut stream = Box::pin(sub.stream());

    let claimed = next(&mut stream).await.expect("claimed delivery");
    assert_eq!(claimed.payload(), b"orphan");
    // One earlier delivery: the one the worker that died never finished.
    assert_eq!(claimed.headers().get_str(DELIVERY_COUNT_HEADER), Some("1"));
    assert!(
        claimed.headers().get_str(IDLE_MS_HEADER).is_some(),
        "a claimed entry reports how long it had been pending"
    );
    claimed.ack().await.expect("ack");

    let fresh = next(&mut stream).await.expect("fresh delivery");
    assert_eq!(fresh.payload(), b"fresh");
    assert_eq!(
        (
            fresh.headers().get_str(DELIVERY_COUNT_HEADER),
            fresh.headers().get_str(IDLE_MS_HEADER)
        ),
        (Some("0"), Some("0")),
        "an entry read off the tail has never been claimed",
    );
    fresh.ack().await.expect("ack");

    drop(stream);
    broker.shutdown().await.expect("shutdown");
}

/// The two counters are the server's, not the decoder's: they are what the pending entries list
/// holds for the same entry at the moment before the claim.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_counters_of_a_claim_are_the_ones_xpending_reports() {
    let Some(url) = env("REDIS_84_TEST_URL") else {
        return;
    };
    let broker = standalone(url).await;
    let key = unique_key("counters");

    orphan(&broker, &key, b"orphan").await;
    let before = pending(&broker, &key, "workers").await;
    let [(pending_id, pending_idle, pending_count)] = before.as_slice() else {
        panic!("the dead worker must leave exactly one pending entry, got {before:?}");
    };

    let mut sub = broker
        .subscribe(
            RedisStream::claiming(&key, CLAIM_EVERYTHING)
                .group("workers")
                .consumer("solo")
                .block(BLOCK),
        )
        .await
        .expect("subscribe claiming");
    let mut stream = Box::pin(sub.stream());
    let claimed = next(&mut stream).await.expect("claimed delivery");

    assert_eq!(claimed.id(), Some(pending_id.as_str()));
    assert_eq!(
        claimed
            .headers()
            .get_str(DELIVERY_COUNT_HEADER)
            .and_then(|raw| raw.parse::<u64>().ok()),
        Some(*pending_count),
        "the count a claim reports is the one the pending list holds before it",
    );
    let idle = claimed
        .headers()
        .get_str(IDLE_MS_HEADER)
        .and_then(|raw| raw.parse::<u64>().ok())
        .expect("a claimed entry reports its idle time");
    assert!(
        idle >= *pending_idle,
        "the idle a claim reports is measured from the same delivery the pending list timed \
         ({idle} < {pending_idle})",
    );
    claimed.ack().await.expect("ack");

    drop(stream);
    broker.shutdown().await.expect("shutdown");
}

/// A retry on this mode appends nothing: the entry stays in the pending entries list under its own
/// id, and the subscription's next read claims it back with the delivery count one higher. That is
/// what makes the server's count a retry counter a handler can cap on.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retry_leaves_the_entry_pending_and_the_next_read_claims_it_back() {
    let Some(url) = env("REDIS_84_TEST_URL") else {
        return;
    };
    let broker = standalone(url).await;
    let key = unique_key("retry");

    let mut sub = broker
        .subscribe(
            RedisStream::claiming(&key, CLAIM_EVERYTHING)
                .group("workers")
                .consumer("solo")
                .block(BLOCK),
        )
        .await
        .expect("subscribe claiming");
    publish(&broker, &key, b"flaky").await;

    let mut stream = Box::pin(sub.stream());
    let first = next(&mut stream).await.expect("first delivery");
    let id = first
        .id()
        .expect("a delivery carries its entry id")
        .to_owned();
    assert_eq!(first.headers().get_str(DELIVERY_COUNT_HEADER), Some("0"));
    assert_eq!(first.payload(), b"flaky");
    first.nack(true).await.expect("retry");

    let pool = broker.pool_handle().expect("a live pool");
    let length: u64 = pool.xlen(key.as_str()).await.expect("xlen");
    assert_eq!(
        length, 1,
        "a retry on this mode appends no copy: the stream still holds the one entry",
    );
    assert_eq!(
        pending(&broker, &key, "workers")
            .await
            .into_iter()
            .map(|(id, _idle, _count)| id)
            .collect::<Vec<_>>(),
        slice::from_ref(&id),
        "the retried entry is still the group's to finish",
    );

    let second = next(&mut stream).await.expect("the claim back");
    assert_eq!(second.id(), Some(id.as_str()));
    assert_eq!(
        second.headers().get_str(DELIVERY_COUNT_HEADER),
        Some("1"),
        "the claim back reports the attempt that did not finish",
    );
    second.ack().await.expect("ack");

    drop(stream);
    broker.shutdown().await.expect("shutdown");
}

/// The refusal is a startup error naming both versions, not a syntax error on the first message.
/// The older standalone of the stand is the server it is checked against.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_claiming_subscription_refuses_to_start_on_an_older_server() {
    let Some(url) = env("REDIS_TEST_URL") else {
        return;
    };
    let broker = standalone(url).await;
    let key = unique_key("too-old");

    let err = broker
        .subscribe(
            RedisStream::claiming(&key, Duration::from_secs(30))
                .group("workers")
                .consumer("solo"),
        )
        .await
        .expect_err("a server without XREADGROUP CLAIM must refuse the subscription");

    assert!(
        matches!(err, RedisError::ServerTooOld(_)),
        "the refusal is about the server, not about the options: {err}"
    );
    let message = err.to_string();
    assert!(
        message.starts_with(&format!(
            "RedisStream::claiming on {key:?} needs Redis 8.4.0 or later"
        )),
        "the refusal names the subscription, the mode and the version it needs: {message}"
    );
    assert!(
        message.contains("the connected server is 7."),
        "the refusal names the version that is connected: {message}"
    );

    // The fresh mode is not checked: every supported server has it.
    let fresh = broker
        .subscribe(RedisStream::new(&key).group("workers"))
        .await;
    assert!(fresh.is_ok(), "the fresh mode must still mount here");

    broker.shutdown().await.expect("shutdown");
}
