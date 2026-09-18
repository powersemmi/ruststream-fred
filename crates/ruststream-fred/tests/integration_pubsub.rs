//! Live tests for the Pub/Sub form against a real server.
//!
//! What lives here is what the in-process stand-in cannot reproduce: `PSUBSCRIBE` glob matching,
//! the two delivery modes that do not interoperate, and the moment the server starts routing a
//! channel to the subscriber. Each case reads the server's own view of its subscriptions
//! (`PUBSUB CHANNELS`, `PUBSUB SHARDCHANNELS`, `PUBSUB NUMPAT`) beside the deliveries, because a
//! transport that keeps nothing cannot be asked afterwards what it did.
//!
//! ```bash
//! just test-brokers
//! ```

use std::collections::BTreeSet;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fred::interfaces::PubsubInterface;
use futures::StreamExt;
use ruststream::{
    AckError, Broker, ConnectedBroker, IncomingMessage, OutgoingMessage, Publisher, Subscriber,
};
use ruststream_fred::{
    ConnectedRedisBroker, PubSubMode, RedisBroker, RedisPubSub, RedisPubSubPattern,
    RedisPubSubPublish,
};

mod live;

const WAIT: Duration = Duration::from_secs(5);

/// How long a case waits before calling a delivery absent. Pub/Sub routes within the round trip
/// the publish already costs, so anything not here by now is not coming.
const QUIET: Duration = Duration::from_millis(300);

fn env(key: &str) -> Option<String> {
    live::url(key)
}

/// A channel name unique to this run. Redis keeps Pub/Sub registrations per server, so a name
/// reused by another case - or by a run whose subscriber has not finished closing - would let one
/// subscriber answer another's publish.
fn unique_channel(base: &str) -> String {
    static RUN: OnceLock<u128> = OnceLock::new();
    static N: AtomicU64 = AtomicU64::new(0);
    let run = RUN.get_or_init(|| {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("a clock after the epoch")
            .as_nanos()
    });
    format!(
        "ruststream-ps.{base}.{run}.{}",
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

async fn none_within<S>(stream: &mut S, label: &str)
where
    S: futures::Stream + Unpin,
{
    let polled = tokio::time::timeout(QUIET, stream.next()).await;
    assert!(polled.is_err(), "{label}: expected no delivery");
}

/// The channels the server is currently routing that match `glob`, from its own registry.
async fn channels(broker: &ConnectedRedisBroker, glob: &str) -> Vec<String> {
    broker
        .pool_handle()
        .expect("live pool")
        .next()
        .pubsub_channels(glob)
        .await
        .expect("pubsub channels")
}

/// The same, for the sharded registry, which is a separate one.
async fn shard_channels(broker: &ConnectedRedisBroker, glob: &str) -> Vec<String> {
    broker
        .pool_handle()
        .expect("live pool")
        .next()
        .pubsub_shardchannels(glob)
        .await
        .expect("pubsub shardchannels")
}

/// How many glob subscriptions the server holds. There is no command that lists them, so a case
/// that needs a pattern's registration compares this count around its own subscribe.
async fn pattern_count(broker: &ConnectedRedisBroker) -> u64 {
    broker
        .pool_handle()
        .expect("live pool")
        .next()
        .pubsub_numpat()
        .await
        .expect("pubsub numpat")
}

async fn publish(broker: &ConnectedRedisBroker, policy: RedisPubSubPublish, channel: &str) {
    broker
        .pubsub_publisher(policy)
        .publish(OutgoingMessage::new(channel, b"hello"), None)
        .await
        .expect("publish");
}

/// A subscribe that has returned is a subscribe the server is already routing, so the publish
/// right after it is delivered rather than dropped on a channel nobody is on yet. The negative
/// half is the transport's own rule: what was published before the subscribe is gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_subscribe_returns_only_once_the_server_routes_the_channel() {
    let Some(url) = env("REDIS_TEST_URL") else {
        return;
    };
    let broker = standalone(url).await;
    let channel = unique_channel("confirmed");

    assert!(
        channels(&broker, &channel).await.is_empty(),
        "the channel must be idle before anything subscribes to it",
    );
    // Nobody is listening yet, so this one is lost by design and cannot be confused with the one
    // published after the subscribe.
    publish(&broker, RedisPubSubPublish::new(), &channel).await;

    let mut sub = broker
        .subscribe_pubsub(RedisPubSub::new(&channel))
        .await
        .expect("subscribe pubsub");

    assert_eq!(
        channels(&broker, &channel).await,
        vec![channel.clone()],
        "the server must already route the channel when the subscribe returns",
    );

    let mut stream = Box::pin(sub.stream());
    none_within(&mut stream, "the message published before the subscribe").await;

    // One publish, one delivery: no retry loop, because the registration is already in place.
    publish(&broker, RedisPubSubPublish::new(), &channel).await;
    let msg = next(&mut stream).await.expect("delivery ok");
    assert_eq!(msg.payload(), b"hello");

    drop(stream);
    broker.shutdown().await.expect("shutdown");
}

/// Pub/Sub settles nothing, and says so instead of reporting a success it did not perform.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pubsub_delivery_cannot_be_settled() {
    let Some(url) = env("REDIS_TEST_URL") else {
        return;
    };
    let broker = standalone(url).await;
    let channel = unique_channel("unsettleable");

    let mut sub = broker
        .subscribe_pubsub(RedisPubSub::new(&channel))
        .await
        .expect("subscribe pubsub");
    let mut stream = Box::pin(sub.stream());

    publish(&broker, RedisPubSubPublish::new(), &channel).await;
    let first = next(&mut stream).await.expect("delivery ok");
    assert!(
        matches!(first.ack().await, Err(AckError::Unsupported)),
        "a channel has nothing to acknowledge with",
    );

    publish(&broker, RedisPubSubPublish::new(), &channel).await;
    let second = next(&mut stream).await.expect("second delivery");
    assert!(
        matches!(second.nack(true).await, Err(AckError::Unsupported)),
        "a requeue on a channel would be a republish the transport never makes",
    );

    drop(stream);
    broker.shutdown().await.expect("shutdown");
}

/// The glob form: one subscription reads every channel the pattern matches, each delivery naming
/// the concrete channel it arrived on, and a channel outside the glob reaches it not at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pattern_subscription_reads_every_channel_its_glob_matches() {
    let Some(url) = env("REDIS_TEST_URL") else {
        return;
    };
    let broker = standalone(url).await;
    let base = unique_channel("pattern");
    let matching = [format!("{base}.created"), format!("{base}.shipped")];
    let outside = format!("{base}-other.created");

    let before = pattern_count(&broker).await;
    let mut sub = broker
        .subscribe_pubsub_pattern(RedisPubSubPattern::new(format!("{base}.*")))
        .await
        .expect("subscribe pattern");
    assert!(
        pattern_count(&broker).await > before,
        "the server must hold the glob subscription once the subscribe returns",
    );

    let mut stream = Box::pin(sub.stream());
    for channel in &matching {
        publish(&broker, RedisPubSubPublish::new(), channel).await;
    }

    // A pool spreads publishes over its connections, so the two channels may arrive either way
    // round; what the glob promises is that both do.
    let mut seen = BTreeSet::new();
    for _ in 0..matching.len() {
        let msg = next(&mut stream).await.expect("matched delivery");
        assert_eq!(msg.payload(), b"hello");
        assert!(
            msg.from_pattern(),
            "a delivery through a glob must report itself as one",
        );
        seen.insert(msg.channel().to_owned());
    }
    assert_eq!(
        seen,
        matching.iter().cloned().collect::<BTreeSet<_>>(),
        "each delivery names the concrete channel it was published to, not the glob",
    );

    // One character off the glob is off the subscription.
    publish(&broker, RedisPubSubPublish::new(), &outside).await;
    none_within(&mut stream, "a channel the glob does not match").await;

    drop(stream);
    broker.shutdown().await.expect("shutdown");
}

/// Sharded delivery is a registry of its own: a sharded subscriber is invisible to `PUBLISH`, and
/// the server says so before any message is sent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_sharded_subscription_is_reached_only_by_a_sharded_publish() {
    let Some(url) = env("REDIS_TEST_URL") else {
        return;
    };
    let broker = standalone(url).await;
    let channel = unique_channel("sharded");

    let mut sub = broker
        .subscribe_pubsub(RedisPubSub::new(&channel).mode(PubSubMode::Sharded))
        .await
        .expect("subscribe sharded");

    assert_eq!(
        shard_channels(&broker, &channel).await,
        vec![channel.clone()],
        "a sharded subscribe registers in the sharded registry",
    );
    assert!(
        channels(&broker, &channel).await.is_empty(),
        "and nowhere else: `PUBLISH` cannot see it",
    );

    let mut stream = Box::pin(sub.stream());

    publish(&broker, RedisPubSubPublish::new(), &channel).await;
    none_within(&mut stream, "a classic publish to a sharded subscriber").await;

    publish(
        &broker,
        RedisPubSubPublish::new().mode(PubSubMode::Sharded),
        &channel,
    )
    .await;
    let msg = next(&mut stream).await.expect("sharded delivery");
    assert_eq!(msg.payload(), b"hello");

    drop(stream);
    broker.shutdown().await.expect("shutdown");
}

/// The other direction of the same rule, so neither mode can be the one that quietly works both
/// ways.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_classic_subscription_is_not_reached_by_a_sharded_publish() {
    let Some(url) = env("REDIS_TEST_URL") else {
        return;
    };
    let broker = standalone(url).await;
    let channel = unique_channel("classic");

    let mut sub = broker
        .subscribe_pubsub(RedisPubSub::new(&channel))
        .await
        .expect("subscribe classic");

    assert_eq!(
        channels(&broker, &channel).await,
        vec![channel.clone()],
        "a classic subscribe registers in the classic registry",
    );
    assert!(
        shard_channels(&broker, &channel).await.is_empty(),
        "and nowhere else: `SPUBLISH` cannot see it",
    );

    let mut stream = Box::pin(sub.stream());

    publish(
        &broker,
        RedisPubSubPublish::new().mode(PubSubMode::Sharded),
        &channel,
    )
    .await;
    none_within(&mut stream, "a sharded publish to a classic subscriber").await;

    publish(&broker, RedisPubSubPublish::new(), &channel).await;
    let msg = next(&mut stream).await.expect("classic delivery");
    assert_eq!(msg.payload(), b"hello");

    drop(stream);
    broker.shutdown().await.expect("shutdown");
}

/// A channel is shared with whatever else publishes to it, so a value that carries no frame of
/// this crate's arrives as the payload it is, with no headers invented around it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_value_published_by_another_client_arrives_as_the_bare_payload() {
    let Some(url) = env("REDIS_TEST_URL") else {
        return;
    };
    let broker = standalone(url).await;
    let channel = unique_channel("foreign");

    let mut sub = broker
        .subscribe_pubsub(RedisPubSub::new(&channel))
        .await
        .expect("subscribe pubsub");
    let mut stream = Box::pin(sub.stream());

    // A plain `PUBLISH`, the way any other Redis client would make it.
    let _: i64 = broker
        .pool_handle()
        .expect("live pool")
        .next()
        .publish(channel.as_str(), "plain text")
        .await
        .expect("publish");

    let msg = next(&mut stream).await.expect("delivery ok");
    assert_eq!(msg.payload(), b"plain text");
    assert!(
        msg.headers().is_empty(),
        "an unframed value carries no headers, and none are read out of its bytes",
    );

    drop(stream);
    broker.shutdown().await.expect("shutdown");
}

/// Both delivery modes on a cluster, which is where they differ in what they cost: a classic
/// publish is broadcast to every node, a sharded one stays on the slot its channel hashes to.
/// The subscriptions are opened against the cluster the same way a service opens them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cluster_serves_both_delivery_modes() {
    let Some(node) = env("REDIS_CLUSTER_TEST_URL") else {
        return;
    };
    let broker = RedisBroker::cluster([node])
        .connect()
        .await
        .expect("connect to the cluster");

    for mode in [PubSubMode::Classic, PubSubMode::Sharded] {
        let channel = unique_channel("cluster");
        let mut sub = broker
            .subscribe_pubsub(RedisPubSub::new(&channel).mode(mode))
            .await
            .unwrap_or_else(|err| panic!("subscribe {mode:?}: {err}"));
        let mut stream = Box::pin(sub.stream());

        publish(&broker, RedisPubSubPublish::new().mode(mode), &channel).await;
        let msg = next(&mut stream).await.expect("delivery");
        assert_eq!(msg.payload(), b"hello", "{mode:?} lost its message");
        assert_eq!(msg.channel(), channel);

        drop(stream);
    }

    broker.shutdown().await.expect("shutdown");
}
