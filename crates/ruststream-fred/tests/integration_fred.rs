//! Real-Redis integration tests for the `RedisBroker`. Each topology is gated behind its own env
//! var, so the default `cargo test` (none set) is a no-op and needs no server.
//!
//! ```bash
//! just brokers-up
//! REDIS_TEST_URL=redis://127.0.0.1:6379 \
//! REDIS_CLUSTER_TEST_URL=127.0.0.1:7000 \
//! REDIS_SENTINEL_TEST_URL=127.0.0.1:26379 \
//!     cargo test -p ruststream-fred --test integration_fred -- --test-threads=1
//! ```
//!
//! These cover what the handler-stub broker cannot: real consumer groups, `XACK`, the
//! republish-on-nack path, `XAUTOCLAIM` reclaim, and the cluster / sentinel topologies.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use fred::interfaces::KeysInterface;
use futures::StreamExt;
use ruststream::codec::JsonCodec;
use ruststream::runtime::RETRY_COUNT_HEADER;
use ruststream::{Broker, Headers, IncomingMessage, OutgoingMessage, Publisher, Subscriber};
use ruststream_fred::{
    DEAD_LETTER_REASON_HEADER, DELIVERY_COUNT_HEADER, IDLE_MS_HEADER, RedisBroker, RedisList,
    RedisPubSub, RedisStream, StreamStart,
};

const WAIT: Duration = Duration::from_secs(5);

/// Master/service name monitored by the sentinel topology in `docker-compose.test.yml`.
const SENTINEL_SERVICE: &str = "mymaster";

fn env(key: &str) -> Option<String> {
    std::env::var(key).ok()
}

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

async fn connect(broker: &RedisBroker) {
    Broker::connect(broker).await.expect("connect to redis");
}

/// Publish one message, read it off a fresh-tail group, and ack. Shared by every topology.
async fn round_trip(broker: &RedisBroker, key: &str) {
    let mut sub = broker
        .subscribe(RedisStream::new(key).group("workers"))
        .await
        .expect("subscribe");

    let mut headers = Headers::new();
    headers.insert("content-type", "application/json");
    broker
        .publisher()
        .publish(OutgoingMessage::new(key, b"hello").with_headers(headers))
        .await
        .expect("publish");

    let mut stream = Box::pin(sub.stream());
    let msg = next(&mut stream).await.expect("delivery ok");
    assert_eq!(msg.payload(), b"hello");
    // Streams carry headers as native entry fields (`h:<name>` + `_payload`).
    assert_eq!(msg.headers().content_type(), Some("application/json"));
    msg.ack().await.expect("ack");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn standalone_round_trip_with_ack() {
    let Some(url) = env("REDIS_TEST_URL") else {
        return;
    };
    let broker = RedisBroker::standalone(url);
    connect(&broker).await;
    round_trip(&broker, &unique_key("round_trip")).await;
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn standalone_nack_requeue_republishes_to_same_stream() {
    let Some(url) = env("REDIS_TEST_URL") else {
        return;
    };
    let broker = RedisBroker::standalone(url);
    connect(&broker).await;
    let key = unique_key("requeue");

    let mut sub = broker
        .subscribe(RedisStream::new(&key).group("workers"))
        .await
        .expect("subscribe");
    broker
        .publisher()
        .publish(OutgoingMessage::new(key.as_str(), b"retry-me"))
        .await
        .expect("publish");

    let mut stream = Box::pin(sub.stream());
    let first = next(&mut stream).await.expect("first delivery");
    assert_eq!(first.payload(), b"retry-me");
    // Republishes a copy to the tail, then acks the original.
    first.nack(true).await.expect("nack requeue");

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
    let broker = RedisBroker::standalone(url);
    connect(&broker).await;
    let key = unique_key("reclaim");

    // A fresh-tail consumer reads the entry but never acks it (the handle is dropped), so it stays
    // in the group's pending list.
    let mut worker = broker
        .subscribe(RedisStream::new(&key).group("workers").consumer("dead"))
        .await
        .expect("subscribe worker");
    broker
        .publisher()
        .publish(OutgoingMessage::new(key.as_str(), b"orphan"))
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

/// Reads and acks the single entry sitting in a dead-letter stream, from the beginning.
async fn read_dead_letter_stream(broker: &RedisBroker, key: &str) -> Headers {
    let mut sub = broker
        .subscribe(
            RedisStream::new(key)
                .group("dlq-readers")
                .start_id(StreamStart::Beginning),
        )
        .await
        .expect("subscribe dlq");
    let mut stream = Box::pin(sub.stream());
    let dead = next(&mut stream).await.expect("dead-letter entry");
    assert_eq!(dead.payload(), b"poison");
    let headers = dead.headers().clone();
    dead.ack().await.expect("ack dlq");
    drop(stream);
    headers
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_drop_routes_to_dead_letter() {
    let Some(url) = env("REDIS_TEST_URL") else {
        return;
    };
    let broker = RedisBroker::standalone(url);
    connect(&broker).await;
    let key = unique_key("dlq_drop");
    let dlq = format!("{key}.dlq");

    let mut sub = broker
        .subscribe(RedisStream::new(&key).group("workers").dead_letter(&dlq))
        .await
        .expect("subscribe");
    broker
        .publisher()
        .publish(OutgoingMessage::new(key.as_str(), b"poison"))
        .await
        .expect("publish");

    let mut stream = Box::pin(sub.stream());
    let first = next(&mut stream).await.expect("delivery");
    first.nack(false).await.expect("drop to dead-letter");
    drop(stream);

    let headers = read_dead_letter_stream(&broker, &dlq).await;
    assert_eq!(headers.get_str(DEAD_LETTER_REASON_HEADER), Some("dropped"));
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_max_deliveries_dead_letters_after_cap() {
    let Some(url) = env("REDIS_TEST_URL") else {
        return;
    };
    let broker = RedisBroker::standalone(url);
    connect(&broker).await;
    let key = unique_key("dlq_cap");
    let dlq = format!("{key}.dlq");

    let mut sub = broker
        .subscribe(
            RedisStream::new(&key)
                .group("workers")
                .dead_letter(&dlq)
                .max_deliveries(2),
        )
        .await
        .expect("subscribe");
    broker
        .publisher()
        .publish(OutgoingMessage::new(key.as_str(), b"poison"))
        .await
        .expect("publish");

    let mut stream = Box::pin(sub.stream());
    // Retry once: count goes 0 -> 1 (< 2), so it is republished.
    next(&mut stream)
        .await
        .expect("delivery 1")
        .nack(true)
        .await
        .expect("requeue");
    // Second delivery carries retry-count 1; nacking it again reaches the cap (2) -> dead-letter.
    let second = next(&mut stream).await.expect("delivery 2");
    assert_eq!(second.headers().get_str(RETRY_COUNT_HEADER), Some("1"));
    second.nack(true).await.expect("requeue past cap");
    drop(stream);

    let headers = read_dead_letter_stream(&broker, &dlq).await;
    assert_eq!(
        headers.get_str(DEAD_LETTER_REASON_HEADER),
        Some("max-deliveries")
    );
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_reclaim_exposes_delivery_count_and_idle() {
    let Some(url) = env("REDIS_TEST_URL") else {
        return;
    };
    let broker = RedisBroker::standalone(url);
    connect(&broker).await;
    let key = unique_key("dlq_meta");

    let mut worker = broker
        .subscribe(RedisStream::new(&key).group("workers").consumer("dead"))
        .await
        .expect("subscribe worker");
    broker
        .publisher()
        .publish(OutgoingMessage::new(key.as_str(), b"stuck"))
        .await
        .expect("publish");
    {
        let mut s = Box::pin(worker.stream());
        drop(next(&mut s).await.expect("worker delivery"));
    }
    drop(worker);

    // A high cap activates the policy (so the counts are exposed) without dead-lettering.
    let mut recovery = broker
        .subscribe(
            RedisStream::reclaim(&key, Duration::from_millis(1))
                .group("workers")
                .consumer("rec")
                .max_deliveries(10)
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
    reclaimed.ack().await.expect("ack");
    drop(stream);
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_reclaim_caps_to_dead_letter() {
    let Some(url) = env("REDIS_TEST_URL") else {
        return;
    };
    let broker = RedisBroker::standalone(url);
    connect(&broker).await;
    let key = unique_key("dlq_reclaim");
    let dlq = format!("{key}.dlq");

    let mut worker = broker
        .subscribe(RedisStream::new(&key).group("workers").consumer("dead"))
        .await
        .expect("subscribe worker");
    broker
        .publisher()
        .publish(OutgoingMessage::new(key.as_str(), b"poison"))
        .await
        .expect("publish");
    {
        let mut s = Box::pin(worker.stream());
        drop(next(&mut s).await.expect("worker delivery"));
    }
    drop(worker);

    // Native delivery count after the reclaim is 2, past the cap of 1, so it is dead-lettered and
    // never delivered: the poll times out with nothing.
    let mut recovery = broker
        .subscribe(
            RedisStream::reclaim(&key, Duration::from_millis(1))
                .group("workers")
                .consumer("rec")
                .dead_letter(&dlq)
                .max_deliveries(1)
                .block(Duration::from_millis(50)),
        )
        .await
        .expect("subscribe recovery");
    let mut stream = Box::pin(recovery.stream());
    let polled = tokio::time::timeout(Duration::from_millis(500), stream.next()).await;
    assert!(polled.is_err(), "a poison reclaim must not be delivered");
    drop(stream);

    let headers = read_dead_letter_stream(&broker, &dlq).await;
    assert_eq!(
        headers.get_str(DEAD_LETTER_REASON_HEADER),
        Some("max-deliveries")
    );
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reliable_list_drop_routes_to_dead_letter() {
    let Some(url) = env("REDIS_TEST_URL") else {
        return;
    };
    let broker = RedisBroker::standalone(url);
    connect(&broker).await;
    let key = unique_key("list_dlq_drop");
    let dlq = format!("{key}.dlq");

    broker
        .list_publisher()
        .publish(OutgoingMessage::new(key.as_str(), b"poison"))
        .await
        .expect("lpush");

    let mut sub = broker
        .subscribe_list(
            RedisList::new(&key)
                .reliable()
                .dead_letter(&dlq)
                .block(Duration::from_millis(50)),
        )
        .await
        .expect("subscribe reliable list");
    let mut stream = Box::pin(sub.stream());
    next(&mut stream)
        .await
        .expect("delivery")
        .nack(false)
        .await
        .expect("drop to dead-letter");
    drop(stream);

    let mut dlq_sub = broker
        .subscribe_list(RedisList::new(&dlq).block(Duration::from_millis(500)))
        .await
        .expect("subscribe dlq list");
    let mut dlq_stream = Box::pin(dlq_sub.stream());
    let dead = next(&mut dlq_stream).await.expect("dead-letter entry");
    assert_eq!(dead.payload(), b"poison");
    assert_eq!(
        dead.headers().get_str(DEAD_LETTER_REASON_HEADER),
        Some("dropped")
    );
    drop(dlq_stream);
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reliable_list_max_deliveries_dead_letters() {
    let Some(url) = env("REDIS_TEST_URL") else {
        return;
    };
    let broker = RedisBroker::standalone(url);
    connect(&broker).await;
    let key = unique_key("list_dlq_cap");
    let dlq = format!("{key}.dlq");

    broker
        .list_publisher()
        .publish(OutgoingMessage::new(key.as_str(), b"poison"))
        .await
        .expect("lpush");

    let mut sub = broker
        .subscribe_list(
            RedisList::new(&key)
                .reliable()
                .dead_letter(&dlq)
                .max_deliveries(2)
                .block(Duration::from_millis(50)),
        )
        .await
        .expect("subscribe reliable list");
    let mut stream = Box::pin(sub.stream());
    next(&mut stream)
        .await
        .expect("delivery 1")
        .nack(true)
        .await
        .expect("requeue");
    let second = next(&mut stream).await.expect("delivery 2");
    assert_eq!(second.headers().get_str(RETRY_COUNT_HEADER), Some("1"));
    second.nack(true).await.expect("requeue past cap");
    drop(stream);

    let mut dlq_sub = broker
        .subscribe_list(RedisList::new(&dlq).block(Duration::from_millis(500)))
        .await
        .expect("subscribe dlq list");
    let mut dlq_stream = Box::pin(dlq_sub.stream());
    let dead = next(&mut dlq_stream).await.expect("dead-letter entry");
    assert_eq!(dead.payload(), b"poison");
    assert_eq!(
        dead.headers().get_str(DEAD_LETTER_REASON_HEADER),
        Some("max-deliveries")
    );
    drop(dlq_stream);
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cluster_round_trip() {
    let Some(node) = env("REDIS_CLUSTER_TEST_URL") else {
        return;
    };
    let broker = RedisBroker::cluster([node]);
    connect(&broker).await;
    round_trip(&broker, &unique_key("cluster")).await;
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sentinel_round_trip() {
    let Some(node) = env("REDIS_SENTINEL_TEST_URL") else {
        return;
    };
    let broker = RedisBroker::sentinel(SENTINEL_SERVICE, [node]);
    connect(&broker).await;
    round_trip(&broker, &unique_key("sentinel")).await;
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pubsub_classic_round_trip() {
    let Some(url) = env("REDIS_TEST_URL") else {
        return;
    };
    let broker = RedisBroker::standalone(url);
    connect(&broker).await;
    let channel = unique_key("pubsub");

    let mut sub = broker
        .subscribe_pubsub(RedisPubSub::new(&channel))
        .await
        .expect("subscribe pubsub");
    let publisher = broker.pubsub_publisher();
    let mut stream = Box::pin(sub.stream());

    // Pub/Sub has no buffering and SUBSCRIBE registers asynchronously, so publish on a retry loop
    // until a delivery lands.
    let mut headers = Headers::new();
    headers.insert("correlation-id", "xyz-1");

    let mut got = None;
    for _ in 0..25 {
        publisher
            .publish(OutgoingMessage::new(channel.as_str(), b"hello").with_headers(headers.clone()))
            .await
            .expect("publish");
        if let Ok(Some(item)) =
            tokio::time::timeout(Duration::from_millis(200), stream.next()).await
        {
            let msg = item.expect("delivery ok");
            // Headers round-trip through the binary envelope (default framing).
            assert_eq!(msg.headers().correlation_id(), Some("xyz-1"));
            got = Some(msg.payload().to_vec());
            break;
        }
    }
    assert_eq!(got.as_deref(), Some(b"hello".as_slice()));

    drop(stream);
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_codec_envelope_round_trips_headers() {
    let Some(url) = env("REDIS_TEST_URL") else {
        return;
    };
    let broker = RedisBroker::standalone(url);
    connect(&broker).await;
    let key = unique_key("list_codec");

    let mut headers = Headers::new();
    headers.insert("content-type", "application/json");

    // Codec on both ends: the wire value is a readable JSON envelope, headers and payload survive.
    broker
        .list_publisher()
        .codec(JsonCodec)
        .publish(OutgoingMessage::new(key.as_str(), br#"{"id":1}"#).with_headers(headers))
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_simple_round_trip() {
    let Some(url) = env("REDIS_TEST_URL") else {
        return;
    };
    let broker = RedisBroker::standalone(url);
    connect(&broker).await;
    let key = unique_key("list_simple");

    broker
        .list_publisher()
        .publish(OutgoingMessage::new(key.as_str(), b"job-1"))
        .await
        .expect("lpush");

    let mut sub = broker
        .subscribe_list(RedisList::new(&key))
        .await
        .expect("subscribe list");
    let mut stream = Box::pin(sub.stream());
    let msg = next(&mut stream).await.expect("delivery ok");
    assert_eq!(msg.payload(), b"job-1");
    // Simple lists are at-most-once: ack is unsupported.
    assert!(msg.ack().await.is_err());

    drop(stream);
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_publisher_ttl_sets_key_expiry() {
    let Some(url) = env("REDIS_TEST_URL") else {
        return;
    };
    let broker = RedisBroker::standalone(url);
    connect(&broker).await;
    let key = unique_key("list_ttl");

    broker
        .list_publisher()
        .ttl(Duration::from_secs(60))
        .publish(OutgoingMessage::new(key.as_str(), b"job"))
        .await
        .expect("lpush with ttl");

    // PTTL returns the remaining lifetime in ms: positive means the key got an expiry.
    let pttl: i64 = broker.pool_handle().pttl(key.as_str()).await.expect("pttl");
    assert!(pttl > 0, "expected a positive key TTL, got {pttl}");

    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_reliable_round_trip_with_ack() {
    let Some(url) = env("REDIS_TEST_URL") else {
        return;
    };
    let broker = RedisBroker::standalone(url);
    connect(&broker).await;
    let key = unique_key("list_reliable");

    let publisher = broker.list_publisher();
    publisher
        .publish(OutgoingMessage::new(key.as_str(), b"job-a"))
        .await
        .expect("lpush a");
    publisher
        .publish(OutgoingMessage::new(key.as_str(), b"job-b"))
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
