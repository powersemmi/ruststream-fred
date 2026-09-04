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

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use fred::interfaces::{ClientLike, KeysInterface};
use fred::types::InfoKind;
use futures::StreamExt;
use ruststream::codec::JsonCodec;
use ruststream::runtime::{PublishExt, RETRY_COUNT_HEADER};
use ruststream::{
    BatchSubscriber, Broker, ConnectedBroker, HeaderMap, IncomingMessage, Outgoing,
    OutgoingMessage, Partitioned, Positioned, Publisher, Seekable, Seeker, Serialized, Subscribe,
    Subscriber, TransactionalPublisher,
};
use ruststream_fred::{
    ConnectedRedisBroker, DEAD_LETTER_REASON_HEADER, DELIVERY_COUNT_HEADER, DelayedRetry,
    IDLE_MS_HEADER, RedisBroker, RedisError, RedisGroupPosition, RedisList, RedisListPublish,
    RedisPubSub, RedisPubSubPublish, RedisPublishExt, RedisStream, StreamStart,
};

const WAIT: Duration = Duration::from_secs(5);

/// Master/service name monitored by the sentinel topology in `docker-compose.test.yml`.
const SENTINEL_SERVICE: &str = "mymaster";

fn env(key: &str) -> Option<String> {
    std::env::var(key).ok()
}

/// An opaque payload: the partition-key case asserts on the header the keyed handle contributes,
/// not on what a codec would make of the body, so the bytes leave as they are.
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
        .publish(OutgoingMessage::new(key.as_str(), b"before"))
        .await
        .expect("publish before shutdown");

    let closed = broker.shutdown().await.expect("shutdown");
    assert!(closed.connections_closed() > 0);

    let err = publisher
        .publish(OutgoingMessage::new(key.as_str(), b"after"))
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
        .publish(OutgoingMessage::new(key.as_str(), b"hello"))
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
async fn read_dead_letter_stream(broker: &ConnectedRedisBroker, key: &str) -> HeaderMap {
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
    let broker = standalone(url).await;
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

/// The partition key set on the publisher survives the `XADD` entry-field encoding, which is what
/// the in-process broker cannot prove: headers travel as prefixed entry fields on a real stream.
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
        .partition_key("tenant-a")
        .message(&Payload(b"payload".to_vec()))
        .to(key.as_str())
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
async fn stream_max_deliveries_dead_letters_after_cap() {
    let Some(url) = env("REDIS_TEST_URL") else {
        return;
    };
    let broker = standalone(url).await;
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
    let broker = standalone(url).await;
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
    let broker = standalone(url).await;
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
    let broker = standalone(url).await;
    let key = unique_key("list_dlq_drop");
    let dlq = format!("{key}.dlq");

    broker
        .list_publisher(RedisListPublish::new())
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
    let broker = standalone(url).await;
    let key = unique_key("list_dlq_cap");
    let dlq = format!("{key}.dlq");

    broker
        .list_publisher(RedisListPublish::new())
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
async fn reliable_list_recovery_returns_orphaned_entry() {
    let Some(url) = env("REDIS_TEST_URL") else {
        return;
    };
    let broker = standalone(url).await;
    let key = unique_key("list_recovery");
    let zset = format!("{key}.inflight");

    broker
        .list_publisher(RedisListPublish::new())
        .publish(OutgoingMessage::new(key.as_str(), b"job-x"))
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
        .publish(OutgoingMessage::new(key.as_str(), b"retry-me"))
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

    // Pub/Sub has no buffering and SUBSCRIBE registers asynchronously, so publish on a retry loop
    // until a delivery lands.
    let mut headers = HeaderMap::new();
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
    let broker = standalone(url).await;
    let key = unique_key("list_codec");

    let mut headers = HeaderMap::new();
    headers.insert("content-type", "application/json");

    // Codec on both ends: the wire value is a readable JSON envelope, headers and payload survive.
    broker
        .list_publisher(RedisListPublish::new().codec(JsonCodec))
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
    let broker = standalone(url).await;
    let key = unique_key("list_simple");

    broker
        .list_publisher(RedisListPublish::new())
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

// A pop returns one entry, so the list subscriber gets `BatchSubscriber` by delegating to the
// core's client-side buffer. This is the same check `conformance::capabilities::batches` makes for
// the stream and Pub/Sub forms; it lives here because the suite names one fixed subject and on
// Redis a list and a stream under one name are the same key.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_pages_are_capped_at_the_size_they_opened_with() {
    const COUNT: u8 = 10;
    // Smaller than the run, so a page that ignored its size would be caught by the assertion
    // rather than by luck of timing.
    const PAGE: NonZeroUsize = NonZeroUsize::new(3).unwrap();

    let Some(url) = env("REDIS_TEST_URL") else {
        return;
    };
    let broker = standalone(url).await;
    let key = unique_key("list_pages");

    let publisher = broker.list_publisher(RedisListPublish::new());
    for i in 0..COUNT {
        publisher
            .publish(OutgoingMessage::new(key.as_str(), &[i]))
            .await
            .expect("lpush");
    }

    let mut sub = broker
        .subscribe_list(RedisList::new(&key).block(Duration::from_millis(50)))
        .await
        .expect("subscribe list");
    let mut pages = Box::pin(sub.batches(PAGE));
    let mut received = Vec::new();
    while received.len() < usize::from(COUNT) {
        let page = next(&mut pages).await.expect("page ok");
        assert!(!page.is_empty(), "a yielded page must not be empty");
        assert!(
            page.len() <= PAGE.get(),
            "a page must never carry more than its size: got {}",
            page.len(),
        );
        received.extend(page.iter().map(|msg| msg.payload().to_vec()));
    }
    let expected: Vec<Vec<u8>> = (0..COUNT).map(|i| vec![i]).collect();
    assert_eq!(received, expected, "pages must preserve the queue order");

    drop(pages);
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
        .publish(OutgoingMessage::new(key.as_str(), b"job"))
        .await
        .expect("lpush with ttl");

    // PTTL returns the remaining lifetime in ms: positive means the key got an expiry.
    let pttl: i64 = broker
        .pool_handle()
        .expect("live pool")
        .pttl(key.as_str())
        .await
        .expect("pttl");
    assert!(pttl > 0, "expected a positive key TTL, got {pttl}");

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
            .publish(OutgoingMessage::new(key.as_str(), payload))
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
        .publish(OutgoingMessage::new(key.as_str(), b"h3"))
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
        .publish(OutgoingMessage::new(key.as_str(), b"g1"))
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
        .publish(OutgoingMessage::new(key.as_str(), b"in-flight"))
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
        .publish(OutgoingMessage::new(key.as_str(), b"c1"))
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
            .publish(OutgoingMessage::new(key.as_str(), payload))
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
