//! Real-Redis integration tests for the `RedisBroker`. Each topology is gated behind its own env
//! var, so the default `cargo test` (none set) is a no-op and needs no server.
//!
//! ```bash
//! just brokers-up
//! REDIS_TEST_URL=redis://127.0.0.1:6379 \
//! REDIS_AUTH_TEST_URL=redis://127.0.0.1:6385 \
//! REDIS_CLUSTER_TEST_URL=127.0.0.1:7000 \
//! REDIS_SENTINEL_TEST_URL=127.0.0.1:26379 \
//!     cargo test -p ruststream-fred --test integration_fred -- --test-threads=1
//! ```
//!
//! These cover what the handler-stub broker cannot: real consumer groups, `XACK`, the
//! republish-on-nack path, `XAUTOCLAIM` reclaim, builder-set auth, the cluster / sentinel
//! topologies, and the post-shutdown behaviour of a publisher that outlives the connection.

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use fred::clients::Pool;
use fred::interfaces::{ClientLike, KeysInterface, ListInterface, StreamsInterface};
use fred::types::InfoKind;
use fred::types::Value;
use fred::types::config::Config;
use futures::StreamExt;
use ruststream::codec::JsonCodec;
use ruststream::runtime::{PublishExt, RETRY_COUNT_HEADER};
use ruststream::{
    AckError, BatchSubscriber, Broker, ConnectedBroker, HeaderMap, IncomingMessage, Outgoing,
    OutgoingMessage, OwnedTransactions, Partitioned, Positioned, Publisher, Seekable, Seeker,
    Serialized, Subscribe, Subscriber, TransactionalPublisher,
};
use ruststream_fred::{
    ConnectedRedisBroker, DELIVERY_COUNT_HEADER, DelayedRetry, IDLE_MS_HEADER, RedisBroker,
    RedisError, RedisGroupPosition, RedisList, RedisListPublish, RedisPubSub, RedisPubSubPublish,
    RedisPublishSteps, RedisStream, StreamStart,
};

mod live;

const WAIT: Duration = Duration::from_secs(5);

/// Master/service name monitored by the sentinel topology in `docker-compose.test.yml`.
const SENTINEL_SERVICE: &str = "mymaster";

/// The URL of one topology, or `None` to skip. Under `RUSTSTREAM_REQUIRE_LIVE` a missing variable
/// fails the test instead, so a job that started the stand cannot report a suite that never ran.
fn env(key: &str) -> Option<String> {
    live::url(key)
}

/// An opaque payload: the partition-key case asserts on the header the step resolves into, not on
/// what a codec would make of the body, so the bytes leave as they are.
#[derive(Outgoing, Serialized)]
struct Payload(Vec<u8>);

/// A per-process-unique stream key so repeated runs against the same Redis stay isolated.
fn unique_key(base: &str) -> String {
    static N: AtomicU64 = AtomicU64::new(0);
    format!("ruststream-it.{base}.{}", N.fetch_add(1, Ordering::Relaxed))
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

/// Asserts nothing lands on `stream` within a short window (a transaction that has not committed
/// yet, an aborted one).
async fn none_within<S>(stream: &mut S, label: &str)
where
    S: futures::Stream + Unpin,
{
    let polled = tokio::time::timeout(Duration::from_millis(300), stream.next()).await;
    assert!(polled.is_err(), "{label}: expected no delivery yet");
}

async fn connect(broker: RedisBroker) -> ConnectedRedisBroker {
    broker.connect().await.expect("connect to redis")
}

async fn standalone(url: String) -> ConnectedRedisBroker {
    connect(RedisBroker::standalone(url)).await
}

/// Publish one message, read it off a fresh-tail group, and ack. Shared by every topology.
async fn round_trip(broker: &ConnectedRedisBroker, key: &str) {
    let mut sub = broker
        .subscribe(RedisStream::new(key).group("workers"))
        .await
        .expect("subscribe");

    let mut headers = HeaderMap::new();
    headers.insert("content-type", "application/json");
    broker
        .publisher()
        .publish(
            OutgoingMessage::new(key, b"hello").with_headers(headers),
            None,
        )
        .await
        .expect("publish");

    let mut stream = Box::pin(sub.stream());
    let msg = next(&mut stream).await.expect("delivery ok");
    assert_eq!(msg.payload(), b"hello");
    // Streams carry headers as native entry fields (`h:<name>` + `_payload`).
    assert_eq!(msg.headers().content_type(), Some("application/json"));
    msg.ack().await.expect("ack");
}

/// A short blocking read, so a subscription that finds nothing comes back at once instead of
/// sleeping out the five-second default.
const SHORT_BLOCK: Duration = Duration::from_millis(50);

/// What the group still owes, read from the server's own pending entries list.
async fn pending(broker: &ConnectedRedisBroker, key: &str, group: &str) -> Vec<String> {
    let rows: Vec<(String, String, u64, u64)> = broker
        .pool_handle()
        .expect("live pool")
        .xpending(key, group, (0_u64, "-", "+", 10_u64))
        .await
        .expect("xpending");
    rows.into_iter().map(|(id, _, _, _)| id).collect()
}

/// How many entries the stream holds.
async fn stream_len(broker: &ConnectedRedisBroker, key: &str) -> u64 {
    broker
        .pool_handle()
        .expect("live pool")
        .xlen(key)
        .await
        .expect("xlen")
}

/// How many entries the list holds.
async fn list_len(broker: &ConnectedRedisBroker, key: &str) -> u64 {
    broker
        .pool_handle()
        .expect("live pool")
        .llen(key)
        .await
        .expect("llen")
}

/// The key's remaining lifetime in milliseconds: positive with an expiry, `-1` with none, `-2`
/// when the key is gone.
async fn pttl(broker: &ConnectedRedisBroker, key: &str) -> i64 {
    broker
        .pool_handle()
        .expect("live pool")
        .pttl(key)
        .await
        .expect("pttl")
}

/// One field of the `XINFO GROUPS` row for `group`, as the server reports it.
async fn group_field(
    broker: &ConnectedRedisBroker,
    key: &str,
    group: &str,
    field: &str,
) -> Option<String> {
    let rows: Vec<HashMap<String, Value>> = broker
        .pool_handle()
        .expect("live pool")
        .xinfo_groups(key)
        .await
        .expect("xinfo groups");
    rows.into_iter()
        .find(|row| row.get("name").and_then(Value::as_string).as_deref() == Some(group))
        .and_then(|row| row.get(field).and_then(Value::as_string))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn standalone_round_trip_with_ack() {
    let Some(url) = env("REDIS_TEST_URL") else {
        return;
    };
    let broker = standalone(url).await;
    round_trip(&broker, &unique_key("round_trip")).await;
    broker.shutdown().await.expect("shutdown");
}

// A publisher aliases the connection and may outlive it, so it must report the shutdown rather
// than silently succeeding against a dead pool.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publisher_errors_after_shutdown() {
    let Some(url) = env("REDIS_TEST_URL") else {
        return;
    };
    let broker = standalone(url).await;
    let key = unique_key("post_shutdown");
    let publisher = broker.publisher();
    publisher
        .publish(OutgoingMessage::new(key.as_str(), b"before"), None)
        .await
        .expect("publish before shutdown");

    let closed = broker.shutdown().await.expect("shutdown");
    assert!(closed.connections_closed() > 0);

    let err = publisher
        .publish(OutgoingMessage::new(key.as_str(), b"after"), None)
        .await
        .expect_err("publishing through a handle aliasing a closed connection must error");
    assert!(matches!(err, RedisError::ShutDown), "got {err}");
}

// Redis Streams always read through a group, so the bare-string subscriber form needs the
// broker-wide default group.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bare_string_subscription_needs_default_group() {
    let Some(url) = env("REDIS_TEST_URL") else {
        return;
    };
    let broker = connect(RedisBroker::standalone(url.clone())).await;
    let err = Subscribe::subscribe(&broker, &unique_key("bare"))
        .await
        .expect_err("a bare-string subscription without a default group must fail");
    assert!(matches!(err, RedisError::InvalidOptions(msg) if msg.contains("default group")));
    broker.shutdown().await.expect("shutdown");

    let broker = connect(RedisBroker::standalone(url).default_group("workers")).await;
    let key = unique_key("bare_ok");
    let mut sub = Subscribe::subscribe(&broker, &key)
        .await
        .expect("subscribe with the default group");
    broker
        .publisher()
        .publish(OutgoingMessage::new(key.as_str(), b"hello"), None)
        .await
        .expect("publish");
    let mut stream = Box::pin(sub.stream());
    let msg = next(&mut stream).await.expect("delivery ok");
    assert_eq!(msg.payload(), b"hello");
    msg.ack().await.expect("ack");
    drop(stream);
    broker.shutdown().await.expect("shutdown");
}

// ACL user + password set via the builder (not the URL) must authenticate and round-trip.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn standalone_auth_round_trip_with_credentials() {
    let Some(url) = env("REDIS_AUTH_TEST_URL") else {
        return;
    };
    let broker = connect(RedisBroker::standalone(url).credentials("worker", "workerpass")).await;
    round_trip(&broker, &unique_key("auth_creds")).await;
    broker.shutdown().await.expect("shutdown");
}

// Password-only AUTH (the default user's requirepass), again set via the builder.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn standalone_auth_round_trip_with_password() {
    let Some(url) = env("REDIS_AUTH_TEST_URL") else {
        return;
    };
    let broker = connect(RedisBroker::standalone(url).password("s3cr3t")).await;
    round_trip(&broker, &unique_key("auth_pass")).await;
    broker.shutdown().await.expect("shutdown");
}

// Connecting to an auth-required server without credentials must fail, not hang or pass.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn standalone_auth_without_credentials_fails() {
    let Some(url) = env("REDIS_AUTH_TEST_URL") else {
        return;
    };
    let result = RedisBroker::standalone(url).connect().await;
    assert!(result.is_err(), "connecting without credentials must fail");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn standalone_nack_requeue_republishes_to_same_stream() {
    let Some(url) = env("REDIS_TEST_URL") else {
        return;
    };
    let broker = standalone(url).await;
    let key = unique_key("requeue");

    let mut sub = broker
        .subscribe(RedisStream::new(&key).group("workers"))
        .await
        .expect("subscribe");
    broker
        .publisher()
        .publish(OutgoingMessage::new(key.as_str(), b"retry-me"), None)
        .await
        .expect("publish");

    let mut stream = Box::pin(sub.stream());
    let first = next(&mut stream).await.expect("first delivery");
    assert_eq!(first.payload(), b"retry-me");
    // Republishes a copy to the tail, then acks the original.
    first.nack(true).await.expect("nack requeue");
    assert_eq!(
        stream_len(&broker, &key).await,
        2,
        "a requeue on this mode is a second entry, not a redelivery of the first",
    );
    assert!(
        pending(&broker, &key, "workers").await.is_empty(),
        "and the original is acknowledged, so the group owes nothing on it",
    );

    let second = next(&mut stream).await.expect("redelivery");
    assert_eq!(second.payload(), b"retry-me");
    second.ack().await.expect("ack");

    drop(stream);
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn standalone_reclaim_picks_up_pending_entries() {
    let Some(url) = env("REDIS_TEST_URL") else {
        return;
    };
    let broker = standalone(url).await;
    let key = unique_key("reclaim");

    // A fresh-tail consumer reads the entry but never acks it (the handle is dropped), so it stays
    // in the group's pending list.
    let mut worker = broker
        .subscribe(RedisStream::new(&key).group("workers").consumer("dead"))
        .await
        .expect("subscribe worker");
    broker
        .publisher()
        .publish(OutgoingMessage::new(key.as_str(), b"orphan"), None)
        .await
        .expect("publish");
    {
        let mut stream = Box::pin(worker.stream());
        let msg = next(&mut stream).await.expect("worker delivery");
        assert_eq!(msg.payload(), b"orphan");
        drop(msg);
    }
    drop(worker);

    // A reclaim consumer with a tiny idle threshold claims the orphaned entry.
    let mut recovery = broker
        .subscribe(
            RedisStream::reclaim(&key, Duration::from_millis(1))
                .group("workers")
                .consumer("recovery")
                // Short poll interval so an empty first claim (entry not yet idle) retries quickly
                // rather than sleeping the 5s default.
                .block(Duration::from_millis(50)),
        )
        .await
        .expect("subscribe recovery");
    let mut stream = Box::pin(recovery.stream());
    let reclaimed = next(&mut stream).await.expect("reclaimed delivery");
    assert_eq!(reclaimed.payload(), b"orphan");
    reclaimed.ack().await.expect("ack");

    drop(stream);
    broker.shutdown().await.expect("shutdown");
}

/// The partition key the step set survives the `XADD` entry-field encoding, which is what the
/// in-process broker cannot prove: headers travel as prefixed entry fields on a real stream.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_partition_key_survives_the_round_trip() {
    let Some(url) = env("REDIS_TEST_URL") else {
        return;
    };
    let broker = standalone(url).await;
    let key = unique_key("partition_key");

    let mut sub = broker
        .subscribe(RedisStream::new(&key).group("workers"))
        .await
        .expect("subscribe");

    broker
        .publisher()
        .message(&Payload(b"payload".to_vec()))
        .to(key.as_str())
        .partition_key("tenant-a")
        .publish()
        .await
        .expect("publish");

    let mut stream = Box::pin(sub.stream());
    let msg = next(&mut stream).await.expect("delivery");
    assert_eq!(
        Partitioned::partition_key(&msg),
        Some(b"tenant-a".as_slice())
    );
    // What the runtime's keyed lanes actually read, off a real stream entry.
    assert_eq!(
        IncomingMessage::partition_key(&msg),
        Some(b"tenant-a".as_slice())
    );
    msg.ack().await.expect("ack");
    drop(stream);
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_reclaim_reports_the_servers_delivery_count() {
    let Some(url) = env("REDIS_TEST_URL") else {
        return;
    };
    let broker = standalone(url).await;
    let key = unique_key("dlq_meta");

    let mut worker = broker
        .subscribe(RedisStream::new(&key).group("workers").consumer("dead"))
        .await
        .expect("subscribe worker");
    broker
        .publisher()
        .publish(OutgoingMessage::new(key.as_str(), b"stuck"), None)
        .await
        .expect("publish");
    {
        let mut s = Box::pin(worker.stream());
        drop(next(&mut s).await.expect("worker delivery"));
    }
    drop(worker);

    let mut recovery = broker
        .subscribe(
            RedisStream::reclaim(&key, Duration::from_millis(1))
                .group("workers")
                .consumer("rec")
                .block(Duration::from_millis(50)),
        )
        .await
        .expect("subscribe recovery");
    let mut stream = Box::pin(recovery.stream());
    let reclaimed = next(&mut stream).await.expect("reclaimed delivery");
    assert_eq!(reclaimed.payload(), b"stuck");
    // Delivered once to the dead worker, then claimed here: native delivery count 2.
    assert_eq!(
        reclaimed.headers().get_str(DELIVERY_COUNT_HEADER),
        Some("2")
    );
    assert!(reclaimed.headers().get_str(IDLE_MS_HEADER).is_some());
    // The count the runtime caps on is the same one, read from the server rather than a header.
    assert_eq!(
        reclaimed.redelivery_count(),
        Some(2),
        "a reclaimed delivery must report the server's own count",
    );
    reclaimed.ack().await.expect("ack");
    drop(stream);
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reliable_list_recovery_returns_orphaned_entry() {
    let Some(url) = env("REDIS_TEST_URL") else {
        return;
    };
    let broker = standalone(url).await;
    let key = unique_key("list_recovery");
    let zset = format!("{key}.inflight");

    broker
        .list_publisher(RedisListPublish::new())
        .publish(OutgoingMessage::new(key.as_str(), b"job-x"), None)
        .await
        .expect("lpush");

    let mut sub = broker
        .subscribe_list(
            RedisList::new(&key)
                .reliable()
                .min_idle(Duration::from_millis(50))
                .recovery_zset(zset)
                // Tight block so the in-loop watchdog polls frequently.
                .block(Duration::from_millis(50)),
        )
        .await
        .expect("subscribe reliable list with recovery");

    let mut stream = Box::pin(sub.stream());
    // Claim the entry, then drop the handle without acking: a dead consumer leaves it stranded on
    // the processing list, tracked in the recovery ZSET.
    let first = next(&mut stream).await.expect("first claim");
    assert_eq!(first.payload(), b"job-x");
    drop(first);

    // Once it has been idle past min_idle, the watchdog returns it to the main list and the same
    // subscription re-claims it.
    let recovered = next(&mut stream).await.expect("recovered redelivery");
    assert_eq!(recovered.payload(), b"job-x");
    recovered.ack().await.expect("ack");

    drop(stream);
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delayed_retry_zset_redelivers_after_delay_with_incremented_count() {
    let Some(url) = env("REDIS_TEST_URL") else {
        return;
    };
    let broker = standalone(url).await;
    let key = unique_key("delayed");
    let zset = format!("{key}.delayed");

    let mut sub = broker
        .subscribe(
            RedisStream::new(&key)
                .group("workers")
                .delayed_retry(DelayedRetry::DurableZset {
                    key: zset,
                    ttl: None,
                })
                // Tight block so the in-loop sweeper polls the delay ZSET frequently.
                .block(Duration::from_millis(50)),
        )
        .await
        .expect("subscribe");
    broker
        .publisher()
        .publish(OutgoingMessage::new(key.as_str(), b"retry-me"), None)
        .await
        .expect("publish");

    let mut stream = Box::pin(sub.stream());
    let first = next(&mut stream).await.expect("first delivery");
    assert_eq!(first.payload(), b"retry-me");
    // Native durable delay: ZADD to the delay ZSET, XACK the original.
    first
        .nack_after(Duration::from_millis(200))
        .await
        .expect("nack_after schedules the delayed retry");

    // The sweeper replays the due entry; the redelivery carries retry-count 1.
    let second = next(&mut stream).await.expect("redelivery after the delay");
    assert_eq!(second.payload(), b"retry-me");
    assert_eq!(second.headers().get_str(RETRY_COUNT_HEADER), Some("1"));
    second.ack().await.expect("ack");

    drop(stream);
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cluster_round_trip() {
    let Some(node) = env("REDIS_CLUSTER_TEST_URL") else {
        return;
    };
    let broker = connect(RedisBroker::cluster([node])).await;
    round_trip(&broker, &unique_key("cluster")).await;
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sentinel_round_trip() {
    let Some(node) = env("REDIS_SENTINEL_TEST_URL") else {
        return;
    };
    let broker = connect(RedisBroker::sentinel(SENTINEL_SERVICE, [node])).await;
    round_trip(&broker, &unique_key("sentinel")).await;
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pubsub_classic_round_trip() {
    let Some(url) = env("REDIS_TEST_URL") else {
        return;
    };
    let broker = standalone(url).await;
    let channel = unique_key("pubsub");

    let mut sub = broker
        .subscribe_pubsub(RedisPubSub::new(&channel))
        .await
        .expect("subscribe pubsub");
    let publisher = broker.pubsub_publisher(RedisPubSubPublish::new());
    let mut stream = Box::pin(sub.stream());

    // The subscribe returned only once the server was routing the channel, so one publish is one
    // delivery and no retry loop stands between them.
    let mut headers = HeaderMap::new();
    headers.insert("correlation-id", "xyz-1");
    publisher
        .publish(
            OutgoingMessage::new(channel.as_str(), b"hello").with_headers(headers),
            None,
        )
        .await
        .expect("publish");

    let msg = next(&mut stream).await.expect("delivery ok");
    assert_eq!(msg.payload(), b"hello");
    // Headers round-trip through the binary envelope (default framing).
    assert_eq!(msg.headers().correlation_id(), Some("xyz-1"));

    drop(stream);
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_codec_envelope_round_trips_headers() {
    let Some(url) = env("REDIS_TEST_URL") else {
        return;
    };
    let broker = standalone(url).await;
    let key = unique_key("list_codec");

    let mut headers = HeaderMap::new();
    headers.insert("content-type", "application/json");

    // Codec on both ends: the wire value is a readable JSON envelope, headers and payload survive.
    broker
        .list_publisher(RedisListPublish::new().codec(JsonCodec))
        .publish(
            OutgoingMessage::new(key.as_str(), br#"{"id":1}"#).with_headers(headers),
            None,
        )
        .await
        .expect("lpush");

    let mut sub = broker
        .subscribe_list(RedisList::new(&key).codec(JsonCodec))
        .await
        .expect("subscribe list");
    let mut stream = Box::pin(sub.stream());
    let msg = next(&mut stream).await.expect("delivery ok");
    assert_eq!(msg.payload(), br#"{"id":1}"#);
    assert_eq!(msg.headers().content_type(), Some("application/json"));

    drop(stream);
    broker.shutdown().await.expect("shutdown");
}

/// The readable envelope carries data that is not text without touching it: these bytes are not
/// valid UTF-8, and the earlier envelope replaced them on the way out.
const NOT_TEXT: &[u8] = &[0xff, 0x00, 0x1f, 0xfe, 0x80];

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_codec_envelope_round_trips_bytes_that_are_not_text() {
    let Some(url) = env("REDIS_TEST_URL") else {
        return;
    };
    let broker = standalone(url).await;
    let key = unique_key("list_codec_binary");

    let mut headers = HeaderMap::new();
    headers.insert("signature", Bytes::from_static(&[0xde, 0xad, 0xbe, 0xef]));

    broker
        .list_publisher(RedisListPublish::new().codec(JsonCodec))
        .publish(
            OutgoingMessage::new(key.as_str(), NOT_TEXT).with_headers(headers),
            None,
        )
        .await
        .expect("lpush");

    let mut sub = broker
        .subscribe_list(RedisList::new(&key).codec(JsonCodec))
        .await
        .expect("subscribe list");
    let mut stream = Box::pin(sub.stream());
    let msg = next(&mut stream).await.expect("delivery ok");

    assert_eq!(
        msg.payload(),
        NOT_TEXT,
        "the list envelope changed the payload"
    );
    assert_eq!(
        msg.headers().get("signature").map(<[u8]>::to_vec),
        Some(vec![0xde, 0xad, 0xbe, 0xef]),
        "the list envelope changed a header value"
    );

    drop(stream);
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pubsub_codec_envelope_round_trips_bytes_that_are_not_text() {
    let Some(url) = env("REDIS_TEST_URL") else {
        return;
    };
    let broker = standalone(url).await;
    let channel = unique_key("pubsub_codec_binary");

    let mut sub = broker
        .subscribe_pubsub(RedisPubSub::new(&channel).codec(JsonCodec))
        .await
        .expect("subscribe pubsub");
    let publisher = broker.pubsub_publisher(RedisPubSubPublish::new().codec(JsonCodec));
    let mut stream = Box::pin(sub.stream());

    let mut headers = HeaderMap::new();
    headers.insert("signature", Bytes::from_static(&[0xde, 0xad, 0xbe, 0xef]));

    publisher
        .publish(
            OutgoingMessage::new(channel.as_str(), NOT_TEXT).with_headers(headers),
            None,
        )
        .await
        .expect("publish");

    let msg = next(&mut stream).await.expect("delivery ok");
    assert_eq!(
        msg.payload(),
        NOT_TEXT,
        "the Pub/Sub envelope changed the payload"
    );
    assert_eq!(
        msg.headers().get("signature").map(<[u8]>::to_vec),
        Some(vec![0xde, 0xad, 0xbe, 0xef]),
        "the Pub/Sub envelope changed a header value"
    );

    drop(stream);
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_simple_round_trip() {
    let Some(url) = env("REDIS_TEST_URL") else {
        return;
    };
    let broker = standalone(url).await;
    let key = unique_key("list_simple");

    broker
        .list_publisher(RedisListPublish::new())
        .publish(OutgoingMessage::new(key.as_str(), b"job-1"), None)
        .await
        .expect("lpush");

    let mut sub = broker
        .subscribe_list(RedisList::new(&key))
        .await
        .expect("subscribe list");
    let mut stream = Box::pin(sub.stream());
    let msg = next(&mut stream).await.expect("delivery ok");
    assert_eq!(msg.payload(), b"job-1");
    // Simple lists are at-most-once: the entry is gone with the pop, so there is nothing to
    // acknowledge and the delivery says so rather than reporting a settlement it did not make.
    assert!(matches!(msg.ack().await, Err(AckError::Unsupported)));

    drop(stream);
    broker.shutdown().await.expect("shutdown");
}

// A pop returns one entry, so the list subscriber gets `BatchSubscriber` by delegating to the
// core's client-side buffer. This is the same check `conformance::capabilities::batches` makes for
// the stream and Pub/Sub forms; it lives here because the suite names one fixed subject and on
// Redis a list and a stream under one name are the same key.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_batches_are_capped_at_the_size_they_opened_with() {
    const COUNT: u8 = 10;
    // Smaller than the run, so a batch that ignored its size would be caught by the assertion
    // rather than by luck of timing.
    const BATCH: NonZeroUsize = NonZeroUsize::new(3).unwrap();

    let Some(url) = env("REDIS_TEST_URL") else {
        return;
    };
    let broker = standalone(url).await;
    let key = unique_key("list_batches");

    let publisher = broker.list_publisher(RedisListPublish::new());
    for i in 0..COUNT {
        publisher
            .publish(OutgoingMessage::new(key.as_str(), &[i]), None)
            .await
            .expect("lpush");
    }

    let mut sub = broker
        .subscribe_list(RedisList::new(&key).block(Duration::from_millis(50)))
        .await
        .expect("subscribe list");
    let mut batches = Box::pin(sub.batches(BATCH));
    let mut received = Vec::new();
    while received.len() < usize::from(COUNT) {
        let batch = next(&mut batches).await.expect("batch ok");
        assert!(!batch.is_empty(), "a yielded batch must not be empty");
        assert!(
            batch.len() <= BATCH.get(),
            "a batch must never carry more than its size: got {}",
            batch.len(),
        );
        received.extend(batch.iter().map(|msg| msg.payload().to_vec()));
    }
    let expected: Vec<Vec<u8>> = (0..COUNT).map(|i| vec![i]).collect();
    assert_eq!(received, expected, "batches must preserve the queue order");

    drop(batches);
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_publisher_ttl_sets_key_expiry() {
    let Some(url) = env("REDIS_TEST_URL") else {
        return;
    };
    let broker = standalone(url).await;
    let key = unique_key("list_ttl");

    broker
        .list_publisher(RedisListPublish::new().ttl(Duration::from_secs(60)))
        .publish(OutgoingMessage::new(key.as_str(), b"job"), None)
        .await
        .expect("lpush with ttl");

    let remaining = pttl(&broker, &key).await;
    assert!(
        remaining > 0,
        "expected a positive key TTL, got {remaining}"
    );

    broker.shutdown().await.expect("shutdown");
}

// The owned transaction contract itself (independent buffers, visibility only on commit, order
// within a buffer, abort, direct publish alongside) is covered by
// `conformance::capabilities::owned_transactions` in `conformance_fred.rs`. What stays here is the
// crate-specific typed sugar over that kind: the buffer encodes each value with the default codec.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_publisher_opens_owned_transactions() {
    let Some(url) = env("REDIS_TEST_URL") else {
        return;
    };
    let broker = standalone(url).await;
    let key = unique_key("owned_typed");

    let mut sub = broker
        .subscribe(RedisStream::new(&key).group("workers"))
        .await
        .expect("subscribe");

    let publisher = broker.publisher();
    let mut txn = publisher.owned_transaction().await.expect("open typed txn");
    txn.publish(key.as_str(), &7_u32).await.expect("buffer 7");

    let mut stream = Box::pin(sub.stream());
    none_within(&mut stream, "typed before commit").await;

    txn.commit().await.expect("commit typed txn");
    let msg = next(&mut stream).await.expect("typed delivery");
    assert_eq!(
        msg.payload(),
        b"7",
        "the publisher's codec encoded the value"
    );
    msg.ack().await.expect("ack");

    drop(stream);
    broker.shutdown().await.expect("shutdown");
}

// The core `capabilities::seeking` suite covers the capability contract (replay from a captured
// position, skipping forward, live deliveries afterwards). What follows is Redis-specific: the
// constructor positions, the group-wide reach of a seek, and what a seek deliberately leaves alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn seek_to_beginning_replays_retained_history() {
    let Some(url) = env("REDIS_TEST_URL") else {
        return;
    };
    let broker = standalone(url).await;
    let key = unique_key("seek_beginning");

    let mut sub = broker
        .subscribe(
            RedisStream::new(&key)
                .group("workers")
                .block(Duration::from_millis(50)),
        )
        .await
        .expect("subscribe");
    let seeker = sub.seeker();

    let publisher = broker.publisher();
    for payload in [b"h1".as_slice(), b"h2"] {
        publisher
            .publish(OutgoingMessage::new(key.as_str(), payload), None)
            .await
            .expect("publish");
    }

    let mut stream = Box::pin(sub.stream());
    for expected in [b"h1".as_slice(), b"h2"] {
        let msg = next(&mut stream).await.expect("initial delivery");
        assert_eq!(msg.payload(), expected);
        msg.ack().await.expect("ack");
    }

    // Everything the stream still retains, acked or not, is delivered again.
    seeker
        .seek(RedisGroupPosition::beginning())
        .await
        .expect("seek to the beginning");
    for expected in [b"h1".as_slice(), b"h2"] {
        let msg = next(&mut stream).await.expect("replayed delivery");
        assert_eq!(msg.payload(), expected, "the whole history must replay");
        msg.ack().await.expect("ack");
    }

    // `end()` parks the group at the tail: the same history is not replayed again.
    seeker
        .seek(RedisGroupPosition::end())
        .await
        .expect("seek to the end");
    none_within(&mut stream, "after seeking to the end").await;
    publisher
        .publish(OutgoingMessage::new(key.as_str(), b"h3"), None)
        .await
        .expect("publish after the seek");
    let live = next(&mut stream).await.expect("delivery after the seek");
    assert_eq!(live.payload(), b"h3");
    live.ack().await.expect("ack");

    drop(stream);
    broker.shutdown().await.expect("shutdown");
}

// The property that sets Redis apart: the cursor belongs to the consumer group, so a seek through
// one subscription's seeker repositions every consumer of that group.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_seek_moves_the_whole_group() {
    let Some(url) = env("REDIS_TEST_URL") else {
        return;
    };
    let broker = standalone(url).await;
    let key = unique_key("seek_group");

    let worker = broker
        .subscribe(
            RedisStream::new(&key)
                .group("workers")
                .consumer("worker")
                .block(Duration::from_millis(50)),
        )
        .await
        .expect("subscribe worker");
    // A second consumer of the same group, which never reads: it only mints the seeker.
    let admin = broker
        .subscribe(
            RedisStream::new(&key)
                .group("workers")
                .consumer("admin")
                .block(Duration::from_millis(50)),
        )
        .await
        .expect("subscribe admin");
    let admin_seeker = admin.seeker();

    broker
        .publisher()
        .publish(OutgoingMessage::new(key.as_str(), b"g1"), None)
        .await
        .expect("publish");

    let mut worker = worker;
    let mut stream = Box::pin(worker.stream());
    let first = next(&mut stream).await.expect("first delivery");
    assert_eq!(first.payload(), b"g1");
    first.ack().await.expect("ack");

    // The admin's seek rewinds the group, so the worker - which never asked - reads the entry
    // again.
    admin_seeker
        .seek(RedisGroupPosition::beginning())
        .await
        .expect("seek through the admin subscription");
    let replayed = next(&mut stream).await.expect("replayed delivery");
    assert_eq!(
        replayed.payload(),
        b"g1",
        "a seek on one consumer must move the group's cursor for all of them"
    );
    replayed.ack().await.expect("ack");

    drop(stream);
    drop(admin);
    broker.shutdown().await.expect("shutdown");
}

// Moving the cursor is not a way to discard work in flight: entries already delivered and not
// acknowledged stay in the pending list and remain reachable through the reclaim path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_seek_leaves_the_pending_list_alone() {
    let Some(url) = env("REDIS_TEST_URL") else {
        return;
    };
    let broker = standalone(url).await;
    let key = unique_key("seek_pending");

    let mut worker = broker
        .subscribe(
            RedisStream::new(&key)
                .group("workers")
                .consumer("dead")
                .block(Duration::from_millis(50)),
        )
        .await
        .expect("subscribe worker");
    let seeker = worker.seeker();

    broker
        .publisher()
        .publish(OutgoingMessage::new(key.as_str(), b"in-flight"), None)
        .await
        .expect("publish");
    {
        // Read without acking, then abandon the consumer: the entry stays pending.
        let mut stream = Box::pin(worker.stream());
        let msg = next(&mut stream).await.expect("delivery");
        assert_eq!(msg.payload(), b"in-flight");
        drop(msg);
    }
    // Skipping the group to the tail must not make the unacked entry unreachable.
    seeker
        .seek(RedisGroupPosition::end())
        .await
        .expect("seek to the end");
    drop(worker);

    let mut recovery = broker
        .subscribe(
            RedisStream::reclaim(&key, Duration::from_millis(1))
                .group("workers")
                .consumer("recovery")
                .block(Duration::from_millis(50)),
        )
        .await
        .expect("subscribe recovery");
    let mut stream = Box::pin(recovery.stream());
    let reclaimed = next(&mut stream).await.expect("reclaimed delivery");
    assert_eq!(
        reclaimed.payload(),
        b"in-flight",
        "the pending entry must survive a cursor move"
    );
    reclaimed.ack().await.expect("ack");

    drop(stream);
    broker.shutdown().await.expect("shutdown");
}

// `XGROUP SETID` is a single-key command, so it works on a cluster too: the stream and its group
// live on one slot.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cluster_seek_replays_history() {
    let Some(node) = env("REDIS_CLUSTER_TEST_URL") else {
        return;
    };
    let broker = connect(RedisBroker::cluster([node])).await;
    let key = unique_key("cluster_seek");

    let mut sub = broker
        .subscribe(
            RedisStream::new(&key)
                .group("workers")
                .block(Duration::from_millis(50)),
        )
        .await
        .expect("subscribe");
    let seeker = sub.seeker();

    broker
        .publisher()
        .publish(OutgoingMessage::new(key.as_str(), b"c1"), None)
        .await
        .expect("publish");

    let mut stream = Box::pin(sub.stream());
    let first = next(&mut stream).await.expect("first delivery");
    let position = first.position();
    assert_eq!(first.payload(), b"c1");
    first.ack().await.expect("ack");

    // Seeking to the captured position redelivers exactly that entry.
    seeker.seek(position).await.expect("seek to the position");
    let replayed = next(&mut stream).await.expect("replayed delivery");
    assert_eq!(replayed.payload(), b"c1");
    replayed.ack().await.expect("ack");

    drop(stream);
    broker.shutdown().await.expect("shutdown");
}

/// How many `EXEC` calls the server has served, read from `INFO commandstats`.
async fn exec_calls(broker: &ConnectedRedisBroker) -> u64 {
    let info: String = broker
        .pool_handle()
        .expect("live pool")
        .next()
        .info(Some(InfoKind::CommandStats))
        .await
        .expect("info commandstats");
    info.lines()
        .find_map(|line| line.strip_prefix("cmdstat_exec:calls="))
        .and_then(|rest| rest.split(',').next())
        .and_then(|calls| calls.trim().parse().ok())
        .unwrap_or(0)
}

// The visibility race a transaction rules out ("subscribers see all entries or none") cannot be
// observed deterministically from a client: any read either precedes or follows the block. What is
// deterministic is the mechanism that provides it - a borrowed commit must reach the server as ONE
// EXEC block, not as N standalone writes the way a pipeline would - so that is what this asserts,
// alongside the whole buffer arriving in publish order.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn borrowed_commit_is_one_exec_block() {
    let Some(url) = env("REDIS_TEST_URL") else {
        return;
    };
    let broker = standalone(url).await;
    let key = unique_key("borrowed_exec");

    let mut sub = broker
        .subscribe(RedisStream::new(&key).group("workers"))
        .await
        .expect("subscribe");

    let publisher = broker.publisher();
    let before = exec_calls(&broker).await;

    publisher.begin_transaction().await.expect("begin");
    for payload in [b"t1".as_slice(), b"t2", b"t3"] {
        publisher
            .publish(OutgoingMessage::new(key.as_str(), payload), None)
            .await
            .expect("buffer");
    }
    let mut stream = Box::pin(sub.stream());
    none_within(&mut stream, "borrowed before commit").await;

    publisher.commit().await.expect("commit");

    for expected in [b"t1".as_slice(), b"t2", b"t3"] {
        let msg = next(&mut stream).await.expect("committed delivery");
        assert_eq!(msg.payload(), expected, "commit preserves publish order");
        msg.ack().await.expect("ack");
    }

    let after = exec_calls(&broker).await;
    assert_eq!(
        after - before,
        1,
        "the three buffered writes must commit as a single EXEC block"
    );

    drop(stream);
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_reliable_round_trip_with_ack() {
    let Some(url) = env("REDIS_TEST_URL") else {
        return;
    };
    let broker = standalone(url).await;
    let key = unique_key("list_reliable");

    let publisher = broker.list_publisher(RedisListPublish::new());
    publisher
        .publish(OutgoingMessage::new(key.as_str(), b"job-a"), None)
        .await
        .expect("lpush a");
    publisher
        .publish(OutgoingMessage::new(key.as_str(), b"job-b"), None)
        .await
        .expect("lpush b");

    let mut sub = broker
        .subscribe_list(RedisList::new(&key).reliable())
        .await
        .expect("subscribe reliable list");
    let mut stream = Box::pin(sub.stream());

    // FIFO: job-a was pushed first, so it pops first.
    let first = next(&mut stream).await.expect("first");
    assert_eq!(first.payload(), b"job-a");
    first.ack().await.expect("ack a (LREM)");

    let second = next(&mut stream).await.expect("second");
    assert_eq!(second.payload(), b"job-b");
    second.ack().await.expect("ack b (LREM)");

    drop(stream);
    broker.shutdown().await.expect("shutdown");
}

/// Where a group starts is decided once, when the group is created, so the two settings have to
/// part ways on a stream that already holds entries.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_start_id_decides_where_a_new_group_begins() {
    let Some(url) = env("REDIS_TEST_URL") else {
        return;
    };
    let broker = standalone(url).await;
    let key = unique_key("start_id");

    let publisher = broker.publisher();
    for payload in [b"s1".as_slice(), b"s2"] {
        publisher
            .publish(OutgoingMessage::new(key.as_str(), payload), None)
            .await
            .expect("publish");
    }

    // A group created at the beginning owns everything the stream already holds.
    let mut replay = broker
        .subscribe(
            RedisStream::new(&key)
                .group("replay")
                .start_id(StreamStart::Beginning)
                .block(SHORT_BLOCK),
        )
        .await
        .expect("subscribe replay");
    assert_eq!(
        group_field(&broker, &key, "replay", "last-delivered-id").await,
        Some("0-0".to_owned()),
        "a group created at the beginning starts before the first entry",
    );
    let mut replayed = Box::pin(replay.stream());
    for expected in [b"s1".as_slice(), b"s2"] {
        let msg = next(&mut replayed).await.expect("replayed delivery");
        assert_eq!(msg.payload(), expected);
        msg.ack().await.expect("ack");
    }

    // The default starts past it, and the server holds that cursor before any read happens.
    let mut tail = broker
        .subscribe(RedisStream::new(&key).group("tail").block(SHORT_BLOCK))
        .await
        .expect("subscribe tail");
    let last_entry = group_field(&broker, &key, "tail", "last-delivered-id").await;
    assert!(
        last_entry.as_deref().is_some_and(|id| id != "0-0"),
        "a group with the default start begins at the tail, got {last_entry:?}",
    );
    let mut fresh = Box::pin(tail.stream());
    none_within(&mut fresh, "a default-start group over an existing stream").await;

    publisher
        .publish(OutgoingMessage::new(key.as_str(), b"s3"), None)
        .await
        .expect("publish after the group exists");
    let live = next(&mut fresh)
        .await
        .expect("delivery after the group exists");
    assert_eq!(live.payload(), b"s3");
    live.ack().await.expect("ack");

    drop(replayed);
    drop(fresh);
    broker.shutdown().await.expect("shutdown");
}

/// A drop is the other half of the requeue: the entry is acknowledged and nothing takes its
/// place, so the stream neither grows nor keeps owing the group anything.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_drop_acks_the_entry_without_appending_a_copy() {
    let Some(url) = env("REDIS_TEST_URL") else {
        return;
    };
    let broker = standalone(url).await;
    let key = unique_key("drop");

    let mut sub = broker
        .subscribe(RedisStream::new(&key).group("workers").block(SHORT_BLOCK))
        .await
        .expect("subscribe");
    broker
        .publisher()
        .publish(OutgoingMessage::new(key.as_str(), b"poison"), None)
        .await
        .expect("publish");

    let mut stream = Box::pin(sub.stream());
    let msg = next(&mut stream).await.expect("delivery");
    assert_eq!(msg.payload(), b"poison");
    msg.nack(false).await.expect("drop");

    assert_eq!(
        stream_len(&broker, &key).await,
        1,
        "a drop appends no copy: the stream still holds the one entry",
    );
    assert!(
        pending(&broker, &key, "workers").await.is_empty(),
        "and the group owes nothing on it any more",
    );
    none_within(&mut stream, "after a drop").await;

    drop(stream);
    broker.shutdown().await.expect("shutdown");
}

/// Reliable settlement, watched on the two lists it moves entries between: a requeue puts the
/// entry back in the queue, a drop removes it from both.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reliable_list_requeues_to_the_queue_and_drops_off_both_lists() {
    let Some(url) = env("REDIS_TEST_URL") else {
        return;
    };
    let broker = standalone(url).await;
    let key = unique_key("list_settle");
    let processing = format!("{key}.processing");

    broker
        .list_publisher(RedisListPublish::new())
        .publish(OutgoingMessage::new(key.as_str(), b"job"), None)
        .await
        .expect("lpush");

    let mut sub = broker
        .subscribe_list(RedisList::new(&key).reliable().block(SHORT_BLOCK))
        .await
        .expect("subscribe reliable list");
    let mut stream = Box::pin(sub.stream());

    let first = next(&mut stream).await.expect("first claim");
    assert_eq!(first.payload(), b"job");
    assert_eq!(
        (
            list_len(&broker, &key).await,
            list_len(&broker, &processing).await
        ),
        (0, 1),
        "a claim holds the entry on the processing list, off the queue",
    );

    first.nack(true).await.expect("requeue");
    assert_eq!(
        (
            list_len(&broker, &key).await,
            list_len(&broker, &processing).await
        ),
        (1, 0),
        "a requeue returns the entry to the queue and lets go of the claim",
    );

    let second = next(&mut stream).await.expect("redelivery");
    assert_eq!(second.payload(), b"job");
    second.nack(false).await.expect("drop");
    assert_eq!(
        (
            list_len(&broker, &key).await,
            list_len(&broker, &processing).await
        ),
        (0, 0),
        "a drop removes the entry instead of returning it",
    );
    none_within(&mut stream, "after a drop").await;

    drop(stream);
    broker.shutdown().await.expect("shutdown");
}

/// The processing list is a key the descriptor names, and naming one moves the claims there
/// rather than to the default spelling beside it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reliable_list_claims_on_the_processing_key_the_descriptor_names() {
    let Some(url) = env("REDIS_TEST_URL") else {
        return;
    };
    let broker = standalone(url).await;
    let key = unique_key("list_processing");
    let named = format!("{key}.inflight");
    let default = format!("{key}.processing");

    broker
        .list_publisher(RedisListPublish::new())
        .publish(OutgoingMessage::new(key.as_str(), b"job"), None)
        .await
        .expect("lpush");

    let mut sub = broker
        .subscribe_list(
            RedisList::new(&key)
                .reliable()
                .processing(&named)
                .block(SHORT_BLOCK),
        )
        .await
        .expect("subscribe reliable list");
    let mut stream = Box::pin(sub.stream());
    let msg = next(&mut stream).await.expect("claim");

    assert_eq!(
        list_len(&broker, &named).await,
        1,
        "the claim is held on the key the descriptor named",
    );
    assert_eq!(
        list_len(&broker, &default).await,
        0,
        "and the default key is not touched",
    );

    msg.ack().await.expect("ack");
    assert_eq!(
        list_len(&broker, &named).await,
        0,
        "an ack releases the claim from that same key",
    );

    drop(stream);
    broker.shutdown().await.expect("shutdown");
}

/// The recovery watchdog cannot run without an idle threshold, and says so at startup rather than
/// picking one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reliable_list_recovery_without_min_idle_is_refused() {
    let Some(url) = env("REDIS_TEST_URL") else {
        return;
    };
    let broker = standalone(url).await;
    let key = unique_key("list_recovery_refusal");

    let err = broker
        .subscribe_list(RedisList::new(&key).recovery_zset(format!("{key}.inflight")))
        .await
        .expect_err("recovery without an idle threshold must be refused");
    assert!(
        matches!(&err, RedisError::InvalidOptions(msg) if msg.contains("min_idle")),
        "the refusal names the setting that is missing: {err}",
    );

    broker.shutdown().await.expect("shutdown");
}

/// The tracking key expires only when the subscription asked it to, so an abandoned watchdog
/// cleans itself up and a live one is not dropped under a running consumer.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_recovery_ttl_arms_an_expiry_on_the_tracking_key() {
    let Some(url) = env("REDIS_TEST_URL") else {
        return;
    };
    let broker = standalone(url).await;

    // `min_idle` well above the case's own runtime, so the watchdog does not recover the entry
    // while it is being looked at.
    let idle = Duration::from_secs(30);
    let ttl = Duration::from_secs(60);

    for (base, recovery_ttl, expected) in [("with_ttl", Some(ttl), true), ("no_ttl", None, false)] {
        let key = unique_key(base);
        let zset = format!("{key}.inflight");

        broker
            .list_publisher(RedisListPublish::new())
            .publish(OutgoingMessage::new(key.as_str(), b"job"), None)
            .await
            .expect("lpush");

        let mut def = RedisList::new(&key)
            .reliable()
            .min_idle(idle)
            .recovery_zset(&zset)
            .block(SHORT_BLOCK);
        if let Some(ttl) = recovery_ttl {
            def = def.recovery_ttl(ttl);
        }

        let mut sub = broker.subscribe_list(def).await.expect("subscribe");
        let mut stream = Box::pin(sub.stream());
        let msg = next(&mut stream).await.expect("claim");

        let remaining = pttl(&broker, &zset).await;
        if expected {
            assert!(
                remaining > 0,
                "a recovery ttl must arm an expiry on the tracking key, got {remaining}",
            );
        } else {
            assert_eq!(
                remaining, -1,
                "without one the tracking key must outlive every claim",
            );
        }

        msg.ack().await.expect("ack");
        drop(stream);
    }

    broker.shutdown().await.expect("shutdown");
}

/// The same rule for the delay queue: its ttl is the subscription's word, and without one the
/// queue keeps whatever was scheduled into it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delay_queue_ttl_arms_an_expiry_on_the_zset() {
    let Some(url) = env("REDIS_TEST_URL") else {
        return;
    };
    let broker = standalone(url).await;

    // Long enough that the subscription's own sweeper cannot replay the entry mid-case.
    let delay = Duration::from_secs(30);
    let ttl = Duration::from_secs(60);

    for (base, queue_ttl, expected) in [
        ("delay_ttl", Some(ttl), true),
        ("delay_no_ttl", None, false),
    ] {
        let key = unique_key(base);
        let zset = format!("{key}.delayed");

        let mut sub = broker
            .subscribe(
                RedisStream::new(&key)
                    .group("workers")
                    .delayed_retry(DelayedRetry::DurableZset {
                        key: zset.clone(),
                        ttl: queue_ttl,
                    })
                    .block(SHORT_BLOCK),
            )
            .await
            .expect("subscribe");
        broker
            .publisher()
            .publish(OutgoingMessage::new(key.as_str(), b"later"), None)
            .await
            .expect("publish");

        let mut stream = Box::pin(sub.stream());
        let msg = next(&mut stream).await.expect("delivery");
        msg.nack_after(delay).await.expect("schedule the delay");

        let remaining = pttl(&broker, &zset).await;
        if expected {
            assert!(
                remaining > 0,
                "a delay-queue ttl must arm an expiry on the zset, got {remaining}",
            );
        } else {
            assert_eq!(
                remaining, -1,
                "without one the queue must outlive what was scheduled into it",
            );
        }

        drop(stream);
    }

    broker.shutdown().await.expect("shutdown");
}

/// A `MULTI` block cannot span hash slots, so a cluster publisher refuses either kind of
/// transaction at the point it is asked for, and keeps publishing directly.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cluster_refuses_both_kinds_of_transaction() {
    let Some(node) = env("REDIS_CLUSTER_TEST_URL") else {
        return;
    };
    let broker = connect(RedisBroker::cluster([node])).await;
    let key = unique_key("cluster_no_txn");
    let publisher = broker.publisher();

    let borrowed = publisher
        .begin_transaction()
        .await
        .expect_err("a cluster cannot open a handle-level transaction");
    assert!(
        matches!(&borrowed, RedisError::InvalidOptions(msg) if msg.contains("standalone and sentinel")),
        "the refusal names the topologies that can: {borrowed}",
    );

    let owned = publisher.transaction().await.expect_err("nor an owned one");
    assert!(
        matches!(&owned, RedisError::InvalidOptions(msg) if msg.contains("standalone and sentinel")),
        "both kinds answer the same way: {owned}",
    );

    // The refusal is about the transaction, not about the handle.
    let mut sub = broker
        .subscribe(RedisStream::new(&key).group("workers"))
        .await
        .expect("subscribe");
    publisher
        .publish(OutgoingMessage::new(key.as_str(), b"direct"), None)
        .await
        .expect("publish");
    let mut stream = Box::pin(sub.stream());
    let msg = next(&mut stream).await.expect("delivery");
    assert_eq!(msg.payload(), b"direct");
    msg.ack().await.expect("ack");

    drop(stream);
    broker.shutdown().await.expect("shutdown");
}

/// A broker handed an already-built pool adopts it instead of dialing, and the config that pool
/// carries is what the Pub/Sub path dials its own client from.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_broker_over_an_adopted_pool_serves_both_transports() {
    let Some(url) = env("REDIS_TEST_URL") else {
        return;
    };
    let config = Config::from_url(&url).expect("a config from the url");
    let pool = Pool::new(config, None, None, None, 2).expect("build the pool");
    pool.init().await.expect("connect the pool");

    let broker = connect(RedisBroker::from_pool(pool)).await;
    round_trip(&broker, &unique_key("adopted")).await;

    let channel = unique_key("adopted_pubsub");
    let mut sub = broker
        .subscribe_pubsub(RedisPubSub::new(&channel))
        .await
        .expect("subscribe pubsub over the adopted pool");
    let mut stream = Box::pin(sub.stream());
    broker
        .pubsub_publisher(RedisPubSubPublish::new())
        .publish(OutgoingMessage::new(channel.as_str(), b"adopted"), None)
        .await
        .expect("publish");
    let msg = next(&mut stream).await.expect("delivery");
    assert_eq!(msg.payload(), b"adopted");

    drop(stream);
    broker.shutdown().await.expect("shutdown");
}
