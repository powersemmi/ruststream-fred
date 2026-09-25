//! The broker's in-process mode: what `connect_in_process` connects, driven directly where the
//! transport is the subject, and through the `TestApp` harness where the production app is.
//!
//! The direct cases open subscriptions and publishers on the connected form the harness connects,
//! to keep failures localised, and hold the in-process server to what Redis does: consumer groups
//! share a stream's entries between their consumers and each group reads every entry, a list hands
//! each element to one consumer and keeps what nobody has popped yet, Pub/Sub reaches every channel
//! and pattern subscription and keeps nothing, and a write of the wrong type is refused. The
//! harness cases run a service's app unchanged and address the broker by its production type.
//! What only a server can show is covered against a live Redis in `tests/integration_fred.rs`.

#![cfg(feature = "testing")]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures::{Stream, StreamExt};
use ruststream::runtime::{App, AppInfo, HandlerOutcome, PublishExt, RustStream};
use ruststream::subscriber;
use ruststream::testing::{InProcess, TestApp, TestableBroker, expect_published};
use ruststream::{
    AckError, AddressedCopies, BatchSubscriber, ConnectedBroker, HeaderMap, IncomingMessage,
    NamedCopies, Outgoing, OutgoingMessage, OwnedTransactions, Partitioned, Publisher, RawMessage,
    RedeliveryAddressed, Serialized, Subscribe, Subscriber, SubscriptionSource, Transaction,
    TransactionalPublisher, nonzero,
};
use ruststream_fred::{
    ConnectedRedisBroker, PARTITION_KEY_HEADER, RedisBroker, RedisError, RedisList,
    RedisListPublish, RedisMessage, RedisPubSub, RedisPubSubPattern, RedisPubSubPublish,
    RedisPublishSteps, RedisStream, StreamStart,
};
use serde::{Deserialize, Serialize};

const WAIT: Duration = Duration::from_secs(1);

/// How long a case waits to be sure nothing arrives.
const QUIET: Duration = Duration::from_millis(50);

/// The address the service's broker is built with; the in-process mode dials nothing.
const URL: &str = "redis://localhost:6379";

/// The transition the harness connects through: the production broker, connected in process.
async fn connected() -> ConnectedRedisBroker {
    RedisBroker::standalone(URL)
        .connect_in_process()
        .await
        .expect("connect in process")
}

async fn next_payload<S, M>(stream: &mut S) -> Vec<u8>
where
    S: Stream<Item = Result<M, RedisError>> + Unpin,
    M: IncomingMessage,
{
    let msg = next_message(stream).await;
    let payload = msg.payload().to_vec();
    msg.ack().await.ok();
    payload
}

/// The undrained counterpart of [`next_payload`], for cases that assert on headers before settling.
async fn next_message<S, M>(stream: &mut S) -> M
where
    S: Stream<Item = Result<M, RedisError>> + Unpin,
{
    tokio::time::timeout(WAIT, stream.next())
        .await
        .expect("delivery within timeout")
        .expect("stream has next")
        .expect("delivery ok")
}

/// Whether `stream` stays quiet for a while.
async fn stays_quiet<S, M>(stream: &mut S) -> bool
where
    S: Stream<Item = Result<M, RedisError>> + Unpin,
{
    tokio::time::timeout(QUIET, stream.next()).await.is_err()
}

/// A message declaring a header contract: the shape whose publish leaves the builder's headers
/// position occupied, so the partition key cannot ride there.
#[derive(Outgoing, Serialize, Deserialize)]
#[outgoing(name = "orders.keyed", headers = OrderMeta)]
struct KeyedOrder {
    id: u64,
}

#[derive(Serialize, Deserialize)]
struct OrderMeta {
    region: String,
}

/// An opaque payload for the partition-key cases: they assert on the header the step resolves
/// into, not on what a codec would make of the body, so the bytes leave as they are.
#[derive(Outgoing, Serialized)]
struct Payload(Vec<u8>);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pub_sub_round_trip_through_broker_traits() {
    let broker = connected().await;

    let mut subscriber = broker
        .subscribe(RedisStream::new("orders").group("workers"))
        .await
        .expect("subscribe");
    let publisher = broker.publisher();

    publisher
        .publish(OutgoingMessage::new("orders", b"o1"), None)
        .await
        .expect("publish");

    let mut stream = Box::pin(subscriber.stream());
    let got = next_payload(&mut stream).await;
    assert_eq!(got, b"o1");
    drop(stream);

    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn distinct_keys_are_isolated() {
    let broker = connected().await;
    let mut orders = broker
        .subscribe(RedisStream::new("orders").group("workers"))
        .await
        .expect("subscribe orders");
    let mut events = broker
        .subscribe(RedisStream::new("events").group("workers"))
        .await
        .expect("subscribe events");
    let publisher = broker.publisher();

    publisher
        .publish(OutgoingMessage::new("orders", b"o"), None)
        .await
        .expect("publish o");
    publisher
        .publish(OutgoingMessage::new("events", b"e"), None)
        .await
        .expect("publish e");

    let mut orders_stream = Box::pin(orders.stream());
    assert_eq!(next_payload(&mut orders_stream).await, b"o");

    let mut events_stream = Box::pin(events.stream());
    assert_eq!(next_payload(&mut events_stream).await, b"e");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nack_requeue_redelivers_to_same_subscriber() {
    let broker = connected().await;
    let mut subscriber = broker
        .subscribe(RedisStream::new("orders").group("workers"))
        .await
        .expect("subscribe");
    let publisher = broker.publisher();

    publisher
        .publish(OutgoingMessage::new("orders", b"once"), None)
        .await
        .expect("publish");

    let mut stream = Box::pin(subscriber.stream());
    let first = tokio::time::timeout(WAIT, stream.next())
        .await
        .expect("first delivery")
        .expect("stream has next")
        .expect("ok");
    first.nack(true).await.expect("nack requeue");

    let second = tokio::time::timeout(WAIT, stream.next())
        .await
        .expect("redelivery")
        .expect("stream has next")
        .expect("ok");
    assert_eq!(second.payload(), b"once");
    second.ack().await.expect("ack");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn headers_are_propagated_to_subscribers() {
    let broker = connected().await;
    let mut subscriber = broker
        .subscribe(RedisStream::new("orders").group("workers"))
        .await
        .expect("subscribe");
    let publisher = broker.publisher();

    let mut headers = HeaderMap::new();
    headers.insert("content-type", "application/json");
    headers.insert("correlation-id", "abc-1");
    let outgoing = OutgoingMessage::new("orders", b"{}").with_headers(headers);
    publisher.publish(outgoing, None).await.expect("publish");

    let mut stream = Box::pin(subscriber.stream());
    let msg = tokio::time::timeout(WAIT, stream.next())
        .await
        .expect("delivery")
        .expect("stream has next")
        .expect("ok");
    assert_eq!(msg.headers().content_type(), Some("application/json"));
    assert_eq!(msg.headers().correlation_id(), Some("abc-1"));
    msg.ack().await.expect("ack");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn expect_published_observes_publishes() {
    let broker = connected().await;
    let publisher = broker.publisher();
    publisher
        .publish(OutgoingMessage::new("events", b"first"), None)
        .await
        .expect("publish first");
    publisher
        .publish(OutgoingMessage::new("events", b"second"), None)
        .await
        .expect("publish second");
    let observed = expect_published(&broker, "events", 2, Duration::from_secs(1)).await;
    assert_eq!(observed.len(), 2);
    assert_eq!(observed[0].payload(), b"first");
    assert_eq!(observed[1].payload(), b"second");
    broker.shutdown().await.expect("shutdown");
}

// The Subscriber contract (and the conformance helpers) re-enter `stream()` per call.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_can_be_reentered() {
    let broker = connected().await;
    let mut subscriber = broker
        .subscribe(RedisStream::new("orders").group("workers"))
        .await
        .expect("subscribe");
    let publisher = broker.publisher();

    publisher
        .publish(OutgoingMessage::new("orders", b"one"), None)
        .await
        .expect("publish one");
    {
        let mut stream = Box::pin(subscriber.stream());
        assert_eq!(next_payload(&mut stream).await, b"one");
    }

    publisher
        .publish(OutgoingMessage::new("orders", b"two"), None)
        .await
        .expect("publish two");
    let mut stream = Box::pin(subscriber.stream());
    assert_eq!(next_payload(&mut stream).await, b"two");
}

/// Writing the header at the call site stays the portable spelling: no step ran, so the publisher
/// leaves the map alone and the delivery reports what the sender wrote.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partition_key_header_is_surfaced() {
    let broker = connected().await;
    let mut sub = broker
        .subscribe(RedisStream::new("events").group("workers"))
        .await
        .expect("subscribe");

    let mut headers = HeaderMap::new();
    headers.insert(PARTITION_KEY_HEADER, "tenant-a");

    broker
        .publisher()
        .publish(
            OutgoingMessage::new("events", b"payload").with_headers(headers),
            None,
        )
        .await
        .expect("publish");

    let mut stream = Box::pin(sub.stream());
    let msg = tokio::time::timeout(WAIT, stream.next())
        .await
        .expect("delivery")
        .expect("item")
        .expect("ok");

    assert_eq!(
        Partitioned::partition_key(&msg),
        Some(b"tenant-a".as_slice())
    );
    msg.ack().await.ok();
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partition_key_absent_yields_none() {
    let broker = connected().await;
    let mut sub = broker
        .subscribe(RedisStream::new("events.bare").group("workers"))
        .await
        .expect("subscribe");

    broker
        .publisher()
        .publish(OutgoingMessage::new("events.bare", b"payload"), None)
        .await
        .expect("publish");

    let mut stream = Box::pin(sub.stream());
    let msg = tokio::time::timeout(WAIT, stream.next())
        .await
        .expect("delivery")
        .expect("item")
        .expect("ok");

    assert_eq!(Partitioned::partition_key(&msg), None);
    msg.ack().await.ok();
    broker.shutdown().await.expect("shutdown");
}

/// The step is the publish-side counterpart of `Partitioned`: what it sets is what the delivery
/// reports, with no hand-built header map at the call site.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partition_key_step_carries_the_header() {
    let broker = connected().await;
    let mut sub = broker
        .subscribe(RedisStream::new("keyed.plain").group("workers"))
        .await
        .expect("subscribe");
    let publisher = broker.publisher();

    publisher
        .message(&Payload(b"payload".to_vec()))
        .to("keyed.plain")
        .partition_key("tenant-a")
        .publish()
        .await
        .expect("publish");

    let msg = next_message(&mut Box::pin(sub.stream())).await;
    assert_eq!(
        Partitioned::partition_key(&msg),
        Some(b"tenant-a".as_slice())
    );
    // The runtime's keyed lanes read the key through `IncomingMessage`, not through the
    // capability, so the two have to agree or `workers(n, by_key)` sees nothing.
    assert_eq!(
        IncomingMessage::partition_key(&msg),
        Some(b"tenant-a".as_slice())
    );
    msg.ack().await.ok();
    broker.shutdown().await.expect("shutdown");
}

/// The reason the step exists: a message declaring a header contract fills the builder's single
/// headers position with that contract, so a partition key has nowhere else to go. As an option
/// it travels beside the contract instead of competing for that position.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partition_key_step_composes_with_a_header_contract() {
    let broker = connected().await;
    let mut sub = broker
        .subscribe(RedisStream::new("orders.keyed").group("workers"))
        .await
        .expect("subscribe");
    let publisher = broker.publisher();

    publisher
        .message(&KeyedOrder { id: 7 })
        .with_headers(&OrderMeta {
            region: "eu".into(),
        })
        .partition_key("tenant-a")
        .publish()
        .await
        .expect("publish");

    let msg = next_message(&mut Box::pin(sub.stream())).await;
    assert_eq!(
        Partitioned::partition_key(&msg),
        Some(b"tenant-a".as_slice())
    );
    // The contract travelled untouched next to the key.
    assert_eq!(msg.headers().get_str("region"), Some("eu"));
    msg.ack().await.ok();
    broker.shutdown().await.expect("shutdown");
}

/// The key is resolved into the headers the publish already carries, so naming unrelated headers
/// at the call site leaves both the key and those entries in place.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partition_key_step_survives_unrelated_call_site_headers() {
    let broker = connected().await;
    let mut sub = broker
        .subscribe(RedisStream::new("keyed.map").group("workers"))
        .await
        .expect("subscribe");
    let publisher = broker.publisher();

    let mut headers = HeaderMap::new();
    headers.insert("trace-id", "abc");

    publisher
        .message(&Payload(b"payload".to_vec()))
        .with_headers(headers)
        .to("keyed.map")
        .partition_key("tenant-b")
        .publish()
        .await
        .expect("publish");

    let msg = next_message(&mut Box::pin(sub.stream())).await;
    assert_eq!(msg.headers().get_str("trace-id"), Some("abc"));
    assert_eq!(
        Partitioned::partition_key(&msg),
        Some(b"tenant-b".as_slice())
    );
    msg.ack().await.ok();
    broker.shutdown().await.expect("shutdown");
}

/// The step wins: a header map may be a contract the message type declares, while the step names
/// this one message and nothing else, so the resolved key is written over what the map carried.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_step_overrides_a_call_site_partition_key() {
    let broker = connected().await;
    let mut sub = broker
        .subscribe(RedisStream::new("keyed.override").group("workers"))
        .await
        .expect("subscribe");
    let publisher = broker.publisher();

    let mut headers = HeaderMap::new();
    headers.insert(PARTITION_KEY_HEADER, "call-site");

    publisher
        .message(&Payload(b"payload".to_vec()))
        .with_headers(headers)
        .to("keyed.override")
        .partition_key("stepped")
        .publish()
        .await
        .expect("publish");

    let msg = next_message(&mut Box::pin(sub.stream())).await;
    assert_eq!(
        Partitioned::partition_key(&msg),
        Some(b"stepped".as_slice())
    );
    msg.ack().await.ok();
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_drains_in_publish_order() {
    let broker = connected().await;
    let publisher = broker.publisher();
    let mut sub = broker
        .subscribe(RedisStream::new("batch.order").group("workers"))
        .await
        .expect("subscribe");

    let count = 5u8;
    for i in 0..count {
        publisher
            .publish(OutgoingMessage::new("batch.order", &[i]), None)
            .await
            .expect("publish");
    }

    // Opened smaller than the run, so the assertion below reads the contract rather than the
    // publish count: a batch never carries more than the size the subscription named.
    let mut batches = Box::pin(sub.batches(nonzero!(3)));
    let batch = tokio::time::timeout(WAIT, batches.next())
        .await
        .expect("batch within timeout")
        .expect("stream has next")
        .expect("ok batch");

    assert!(!batch.is_empty(), "batch must contain at least one message");
    assert!(batch.len() <= 3, "a batch must not exceed its size");
    for (i, msg) in batch.into_iter().enumerate() {
        assert_eq!(msg.payload(), &[u8::try_from(i).expect("count fits u8")]);
        msg.ack().await.ok();
    }
    broker.shutdown().await.expect("shutdown");
}

// Same re-entry contract as `stream()`: dropping the batch stream and calling `batches()` again
// must keep working.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batches_can_be_reentered() {
    let broker = connected().await;
    let publisher = broker.publisher();
    let mut sub = broker
        .subscribe(RedisStream::new("batch.reenter").group("workers"))
        .await
        .expect("subscribe");

    publisher
        .publish(OutgoingMessage::new("batch.reenter", b"one"), None)
        .await
        .expect("publish");
    {
        let mut batches = Box::pin(sub.batches(nonzero!(8)));
        let batch = tokio::time::timeout(WAIT, batches.next())
            .await
            .expect("batch within timeout")
            .expect("stream has next")
            .expect("ok batch");
        assert_eq!(
            batch.first().map(|m| m.payload().to_vec()),
            Some(b"one".to_vec())
        );
        for msg in batch {
            msg.ack().await.ok();
        }
    }

    publisher
        .publish(OutgoingMessage::new("batch.reenter", b"two"), None)
        .await
        .expect("publish");
    let mut batches = Box::pin(sub.batches(nonzero!(8)));
    let batch = tokio::time::timeout(WAIT, batches.next())
        .await
        .expect("batch within timeout")
        .expect("stream has next")
        .expect("ok batch");
    assert_eq!(
        batch.first().map(|m| m.payload().to_vec()),
        Some(b"two".to_vec())
    );
    for msg in batch {
        msg.ack().await.ok();
    }
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transaction_buffers_until_commit() {
    let broker = connected().await;
    let mut sub = broker
        .subscribe(RedisStream::new("tx").group("workers"))
        .await
        .expect("subscribe");
    let publisher = broker.publisher();

    publisher.begin_transaction().await.expect("begin");
    publisher
        .publish(OutgoingMessage::new("tx", b"first"), None)
        .await
        .expect("publish first");
    publisher
        .publish(OutgoingMessage::new("tx", b"second"), None)
        .await
        .expect("publish second");

    // Nothing is visible before commit.
    let observed = expect_published(&broker, "tx", 1, Duration::from_millis(50)).await;
    assert!(observed.is_empty(), "buffered messages must not be visible");

    publisher.commit().await.expect("commit");

    let mut stream = Box::pin(sub.stream());
    assert_eq!(next_payload(&mut stream).await, b"first");
    assert_eq!(next_payload(&mut stream).await, b"second");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transaction_abort_discards_buffer() {
    let broker = connected().await;
    let publisher = broker.publisher();

    publisher.begin_transaction().await.expect("begin");
    publisher
        .publish(OutgoingMessage::new("tx", b"discarded"), None)
        .await
        .expect("publish");
    publisher.abort().await.expect("abort");

    let observed = expect_published(&broker, "tx", 1, Duration::from_millis(50)).await;
    assert!(observed.is_empty(), "aborted messages must be discarded");
}

// The `TransactionalPublisher` contract requires misuse to surface as an error rather than a
// silent no-op; the in-process publisher mirrors the real one here.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transaction_misuse_errors() {
    let broker = connected().await;
    let publisher = broker.publisher();

    assert!(matches!(
        publisher.commit().await,
        Err(RedisError::NoTransaction)
    ));
    assert!(matches!(
        publisher.abort().await,
        Err(RedisError::NoTransaction)
    ));

    publisher.begin_transaction().await.expect("begin");
    assert!(matches!(
        publisher.begin_transaction().await,
        Err(RedisError::TransactionBusy)
    ));
    // The rejected second begin must leave the open transaction intact.
    publisher
        .publish(OutgoingMessage::new("tx.misuse", b"kept"), None)
        .await
        .expect("publish inside the open transaction");
    publisher.commit().await.expect("commit");

    let observed = expect_published(&broker, "tx.misuse", 1, Duration::from_millis(50)).await;
    assert_eq!(observed.len(), 1);
}

// The owned kind mirrors the real publisher: independent buffers, invisible until each commits,
// with the handle still publishing directly meanwhile.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn owned_transactions_are_independent() {
    let broker = connected().await;
    let publisher = broker.publisher();

    let mut orders = publisher.transaction().await.expect("open orders txn");
    let mut audit = publisher.transaction().await.expect("open audit txn");
    orders
        .publish(OutgoingMessage::new("owned.orders", b"o1"), None)
        .await
        .expect("buffer o1");
    orders
        .publish(OutgoingMessage::new("owned.orders", b"o2"), None)
        .await
        .expect("buffer o2");
    audit
        .publish(OutgoingMessage::new("owned.audit", b"a1"), None)
        .await
        .expect("buffer a1");

    // A direct publish through the same handle is unaffected by the open transactions.
    publisher
        .publish(OutgoingMessage::new("owned.orders", b"direct"), None)
        .await
        .expect("direct publish");
    let observed = expect_published(&broker, "owned.orders", 2, Duration::from_millis(50)).await;
    assert_eq!(
        observed.len(),
        1,
        "only the direct publish is visible before commit"
    );
    assert_eq!(observed[0].payload(), b"direct");

    orders.commit().await.expect("commit orders");
    let observed = expect_published(&broker, "owned.orders", 3, WAIT).await;
    let payloads: Vec<&[u8]> = observed.iter().map(RawMessage::payload).collect();
    assert_eq!(payloads, [b"direct".as_slice(), b"o1", b"o2"]);

    // Settling one transaction leaves the other untouched.
    let audit_before = expect_published(&broker, "owned.audit", 1, Duration::from_millis(50)).await;
    assert!(audit_before.is_empty());
    audit.commit().await.expect("commit audit");
    let audit_after = expect_published(&broker, "owned.audit", 1, WAIT).await;
    assert_eq!(audit_after.len(), 1);
    assert_eq!(audit_after[0].payload(), b"a1");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn owned_transaction_abort_discards_the_buffer() {
    let broker = connected().await;
    let publisher = broker.publisher();

    let mut txn = publisher.transaction().await.expect("open txn");
    txn.publish(OutgoingMessage::new("owned.abort", b"discarded"), None)
        .await
        .expect("buffer");
    txn.abort().await.expect("abort");

    let observed = expect_published(&broker, "owned.abort", 1, Duration::from_millis(50)).await;
    assert!(observed.is_empty(), "aborted messages must be discarded");
}

// What Redis does with a stream, a list and a channel, which the in-process server keeps.

/// Two consumers of one group share the stream: each entry reaches one of them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consumers_of_one_group_share_the_entries() {
    let broker = connected().await;
    let mut first = broker
        .subscribe(RedisStream::new("shared").group("workers").consumer("a"))
        .await
        .expect("subscribe a");
    let mut second = broker
        .subscribe(RedisStream::new("shared").group("workers").consumer("b"))
        .await
        .expect("subscribe b");
    broker
        .publisher()
        .publish(OutgoingMessage::new("shared", b"once"), None)
        .await
        .expect("publish");

    let mut first = Box::pin(first.stream());
    let mut second = Box::pin(second.stream());
    let delivered = tokio::select! {
        msg = first.next() => msg,
        msg = second.next() => msg,
    }
    .expect("stream has next")
    .expect("delivery ok");
    assert_eq!(delivered.payload(), b"once");
    delivered.ack().await.expect("ack");
    assert!(stays_quiet(&mut first).await && stays_quiet(&mut second).await);
}

/// Every group reads every entry of the stream.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_group_reads_every_entry() {
    let broker = connected().await;
    let mut billing = broker
        .subscribe(RedisStream::new("fanned").group("billing"))
        .await
        .expect("subscribe billing");
    let mut shipping = broker
        .subscribe(RedisStream::new("fanned").group("shipping"))
        .await
        .expect("subscribe shipping");
    broker
        .publisher()
        .publish(OutgoingMessage::new("fanned", b"o"), None)
        .await
        .expect("publish");

    assert_eq!(next_payload(&mut Box::pin(billing.stream())).await, b"o");
    assert_eq!(next_payload(&mut Box::pin(shipping.stream())).await, b"o");
}

/// The stream keeps its entries: a group created at the beginning reads what was written before
/// it existed, and a group created at the tail does not.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_group_starts_where_its_descriptor_says() {
    let broker = connected().await;
    broker
        .publisher()
        .publish(OutgoingMessage::new("history", b"old"), None)
        .await
        .expect("publish before any group");

    let mut replaying = broker
        .subscribe(
            RedisStream::new("history")
                .group("replay")
                .start_id(StreamStart::Beginning),
        )
        .await
        .expect("subscribe replay");
    let mut tail = broker
        .subscribe(RedisStream::new("history").group("tail"))
        .await
        .expect("subscribe tail");

    assert_eq!(
        next_payload(&mut Box::pin(replaying.stream())).await,
        b"old"
    );
    assert!(stays_quiet(&mut Box::pin(tail.stream())).await);
}

/// An entry delivered and never acknowledged stays pending on the fresh tail: nothing reads it
/// again, and a claiming consumer of the group takes it once it has been idle long enough.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unacknowledged_entry_stays_pending_until_claimed() {
    let broker = connected().await;
    let mut fresh = broker
        .subscribe(
            RedisStream::new("pending")
                .group("workers")
                .consumer("fresh"),
        )
        .await
        .expect("subscribe fresh");
    broker
        .publisher()
        .publish(OutgoingMessage::new("pending", b"stuck"), None)
        .await
        .expect("publish");
    {
        let mut stream = Box::pin(fresh.stream());
        let msg: RedisMessage = next_message(&mut stream).await;
        drop(msg);
        assert!(
            stays_quiet(&mut stream).await,
            "the fresh tail reads it once"
        );
    }

    let mut reclaim = broker
        .subscribe(
            RedisStream::reclaim("pending", Duration::from_millis(20))
                .group("workers")
                .consumer("rescuer"),
        )
        .await
        .expect("subscribe reclaim");
    let msg: RedisMessage = next_message(&mut Box::pin(reclaim.stream())).await;
    assert_eq!(msg.payload(), b"stuck");
    assert_eq!(
        msg.redelivery_count(),
        Some(2),
        "delivered twice, counting this one"
    );
    msg.ack().await.expect("ack");
}

/// A list keeps what nobody popped, and hands each element to one consumer.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_list_keeps_its_elements_and_hands_each_to_one_consumer() {
    let broker = connected().await;
    let jobs = broker.list_publisher(RedisListPublish::new());
    jobs.publish(OutgoingMessage::new("queue", b"early"), None)
        .await
        .expect("publish before any consumer");

    let mut first = SubscriptionSource::subscribe(RedisList::new("queue"), &broker)
        .await
        .expect("subscribe first");
    let mut second = SubscriptionSource::subscribe(RedisList::new("queue"), &broker)
        .await
        .expect("subscribe second");
    let mut first = Box::pin(first.stream());
    let mut second = Box::pin(second.stream());

    let popped = tokio::select! {
        msg = first.next() => msg,
        msg = second.next() => msg,
    }
    .expect("stream has next")
    .expect("delivery ok");
    assert_eq!(popped.payload(), b"early");
    assert!(stays_quiet(&mut first).await && stays_quiet(&mut second).await);
}

/// A pattern subscription reads every channel its glob matches, and a channel subscription only
/// its own.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pattern_subscription_reads_every_matching_channel() {
    let broker = connected().await;
    let mut glob = SubscriptionSource::subscribe(RedisPubSubPattern::new("events.*"), &broker)
        .await
        .expect("psubscribe");
    let mut exact = SubscriptionSource::subscribe(RedisPubSub::new("events.eu"), &broker)
        .await
        .expect("subscribe");
    let events = broker.pubsub_publisher(RedisPubSubPublish::new());
    events
        .publish(OutgoingMessage::new("events.eu", b"eu"), None)
        .await
        .expect("publish eu");
    events
        .publish(OutgoingMessage::new("events.us", b"us"), None)
        .await
        .expect("publish us");
    events
        .publish(OutgoingMessage::new("orders.eu", b"elsewhere"), None)
        .await
        .expect("publish elsewhere");

    let mut glob = Box::pin(glob.stream());
    let first = next_message(&mut glob).await;
    assert_eq!((first.channel(), first.from_pattern()), ("events.eu", true));
    let second = next_message(&mut glob).await;
    assert_eq!(second.channel(), "events.us");
    assert!(stays_quiet(&mut glob).await);

    let mut exact = Box::pin(exact.stream());
    let only = next_message(&mut exact).await;
    assert_eq!((only.channel(), only.from_pattern()), ("events.eu", false));
    assert!(stays_quiet(&mut exact).await);
}

/// A message published while nobody listens is gone, as on a server.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pubsub_keeps_nothing_for_a_late_subscriber() {
    let broker = connected().await;
    broker
        .pubsub_publisher(RedisPubSubPublish::new())
        .publish(OutgoingMessage::new("late", b"lost"), None)
        .await
        .expect("publish");
    let mut sub = SubscriptionSource::subscribe(RedisPubSub::new("late"), &broker)
        .await
        .expect("subscribe");
    assert!(stays_quiet(&mut Box::pin(sub.stream())).await);
}

/// A name is one Redis type: a list write to a stream is refused, and a stream write to a list.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_write_of_the_wrong_type_is_refused() {
    let broker = connected().await;
    broker
        .publisher()
        .publish(OutgoingMessage::new("typed.stream", b"x"), None)
        .await
        .expect("xadd");
    let err = broker
        .list_publisher(RedisListPublish::new())
        .publish(OutgoingMessage::new("typed.stream", b"x"), None)
        .await
        .expect_err("an LPUSH to a stream");
    assert!(format!("{err}").contains("WRONGTYPE"), "got {err}");

    broker
        .list_publisher(RedisListPublish::new())
        .publish(OutgoingMessage::new("typed.list", b"x"), None)
        .await
        .expect("lpush");
    let err = broker
        .publisher()
        .publish(OutgoingMessage::new("typed.list", b"x"), None)
        .await
        .expect_err("an XADD to a list");
    assert!(format!("{err}").contains("WRONGTYPE"), "got {err}");
}

/// A sharded publish reaches the sharded subscriptions only, and a classic one the classic.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_sharded_publish_reaches_the_sharded_subscriptions_only() {
    let broker = connected().await;
    let mut classic = SubscriptionSource::subscribe(RedisPubSub::new("shards"), &broker)
        .await
        .expect("subscribe");
    let mut sharded = SubscriptionSource::subscribe(
        RedisPubSub::new("shards").mode(ruststream_fred::PubSubMode::Sharded),
        &broker,
    )
    .await
    .expect("ssubscribe");
    broker
        .pubsub_publisher(RedisPubSubPublish::new().mode(ruststream_fred::PubSubMode::Sharded))
        .publish(OutgoingMessage::new("shards", b"s"), None)
        .await
        .expect("spublish");

    assert_eq!(next_payload(&mut Box::pin(sharded.stream())).await, b"s");
    assert!(stays_quiet(&mut Box::pin(classic.stream())).await);
}

/// Where a publish is delivered, as the harness asks the broker in both modes: a stream entry
/// once per consumer group, a list element once, and a channel message once per subscription of
/// the channel and once per matching pattern subscription. Subscriptions sharing a name are told
/// apart by nothing else, so the first of them is owed each delivery.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_publish_is_owed_by_what_redis_routes_it_to() {
    let broker = connected().await;
    let _billing_a = broker
        .subscribe(RedisStream::new("orders").group("billing").consumer("a"))
        .await
        .expect("subscribe");
    let _billing_b = broker
        .subscribe(RedisStream::new("orders").group("billing").consumer("b"))
        .await
        .expect("subscribe");
    let _audit = broker
        .subscribe(RedisStream::new("orders").group("audit"))
        .await
        .expect("subscribe");
    let _jobs = SubscriptionSource::subscribe(RedisList::new("jobs"), &broker)
        .await
        .expect("subscribe");
    let _eu = SubscriptionSource::subscribe(RedisPubSub::new("events.eu"), &broker)
        .await
        .expect("subscribe");
    let _eu_again = SubscriptionSource::subscribe(RedisPubSub::new("events.eu"), &broker)
        .await
        .expect("subscribe");
    let _glob = SubscriptionSource::subscribe(RedisPubSubPattern::new("events.*"), &broker)
        .await
        .expect("psubscribe");
    let names = [
        "orders",
        "orders",
        "orders",
        "jobs",
        "events.eu",
        "events.eu",
        "events.*",
    ];

    assert_eq!(broker.routes("orders", &names), [0, 0], "one per group");
    assert_eq!(broker.routes("jobs", &names), [3], "one consumer");
    assert_eq!(
        broker.routes("events.eu", &names),
        [4, 4, 6],
        "every channel subscription and the matching pattern"
    );
    assert_eq!(
        broker.routes("events.us", &names),
        [6],
        "a name nothing reads exactly reaches the patterns"
    );
    assert!(broker.routes("elsewhere", &names).is_empty());
}

// What the transport refuses. Each is contract behaviour the conformance suites check in process;
// these cases name them, so a regression reads as itself.

/// A handle that outlived the connection refuses instead of writing to a server nobody reads.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_publisher_errors_after_shutdown() {
    let broker = connected().await;
    let publisher = broker.publisher();
    publisher
        .publish(OutgoingMessage::new("post.shutdown", b"before"), None)
        .await
        .expect("publish before shutdown");

    broker.shutdown().await.expect("shutdown");

    let err = publisher
        .publish(OutgoingMessage::new("post.shutdown", b"after"), None)
        .await
        .expect_err("publishing through a handle aliasing a closed connection must error");
    assert!(matches!(err, RedisError::ShutDown), "got {err}");
}

/// Pub/Sub cannot acknowledge on a real server, so it does not acknowledge here either.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pubsub_delivery_cannot_be_settled() {
    let broker = connected().await;
    let mut sub = SubscriptionSource::subscribe(RedisPubSub::new("unsettleable.pubsub"), &broker)
        .await
        .expect("subscribe");

    broker
        .pubsub_publisher(RedisPubSubPublish::new())
        .publish(OutgoingMessage::new("unsettleable.pubsub", b"e"), None)
        .await
        .expect("publish");

    let msg = next_message(&mut Box::pin(sub.stream())).await;
    assert!(matches!(msg.ack().await, Err(AckError::Unsupported)));
}

/// A simple list is at-most-once for the same reason, and a refused requeue does not redeliver.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_simple_list_delivery_cannot_be_settled_or_requeued() {
    let broker = connected().await;
    let mut sub = SubscriptionSource::subscribe(RedisList::new("unsettleable.list"), &broker)
        .await
        .expect("subscribe");

    broker
        .list_publisher(RedisListPublish::new())
        .publish(OutgoingMessage::new("unsettleable.list", b"j"), None)
        .await
        .expect("publish");

    let mut stream = Box::pin(sub.stream());
    let msg = next_message(&mut stream).await;
    assert!(matches!(msg.nack(true).await, Err(AckError::Unsupported)));
    assert!(
        stays_quiet(&mut stream).await,
        "a refused requeue must not redeliver"
    );
}

/// The forms that settle on a server settle here: a requeue on a reliable list pushes the element
/// back, where the next pop takes it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reliable_list_requeues_onto_its_queue() {
    let broker = connected().await;
    let mut sub =
        SubscriptionSource::subscribe(RedisList::new("settleable.list").reliable(), &broker)
            .await
            .expect("subscribe");
    broker
        .list_publisher(RedisListPublish::new())
        .publish(OutgoingMessage::new("settleable.list", b"x"), None)
        .await
        .expect("publish");

    let mut stream = Box::pin(sub.stream());
    let first = next_message(&mut stream).await;
    first.nack(true).await.expect("a reliable list requeues");
    let again = next_message(&mut stream).await;
    assert_eq!(again.payload(), b"x");
    again.ack().await.expect("a reliable list acknowledges");
    assert!(stays_quiet(&mut stream).await);
}

// Where a retry copy is published. Every descriptor that names one destination answers with its
// key or channel, and the conformance ladders hold that answer to its promise. The one that reads
// many, a pattern subscription, declares `NamedCopies` instead, so the mount site owes a
// destination and nothing here has an address to check.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_addressed_descriptor_reports_where_a_retry_copy_goes() {
    let broker = connected().await;

    for (reported, expected) in [
        (
            RedisStream::new("orders")
                .group("workers")
                .redelivery_address(&broker)
                .await,
            "orders",
        ),
        (
            RedisStream::reclaim("orders", Duration::from_secs(30))
                .group("workers")
                .redelivery_address(&broker)
                .await,
            "orders",
        ),
        (
            RedisStream::claiming("orders", Duration::from_secs(30))
                .group("workers")
                .redelivery_address(&broker)
                .await,
            "orders",
        ),
        (
            RedisList::new("jobs")
                .reliable()
                .redelivery_address(&broker)
                .await,
            "jobs",
        ),
        (
            RedisPubSub::new("notifications")
                .redelivery_address(&broker)
                .await,
            "notifications",
        ),
    ] {
        assert_eq!(
            reported
                .expect("reporting an address must not fail")
                .as_str(),
            expected,
        );
    }
}

/// The by-name form takes the broker's own answer, so `#[subscriber("orders")]` needs nothing
/// from the mount site either: the key is both what it reads and what a copy is written to.
#[test]
fn a_bare_name_addresses_its_own_copies() {
    fn addressed<C: Subscribe<Copies = AddressedCopies>>() {}
    addressed::<ConnectedRedisBroker>();
}

/// A pattern reads many channels and a glob is not a channel a `PUBLISH` can name, so the
/// descriptor declares that the mount site names the destination.
#[test]
fn a_pattern_leaves_the_destination_to_the_mount_site() {
    fn named<C, S>()
    where
        C: ConnectedBroker,
        S: SubscriptionSource<C, Copies = NamedCopies>,
    {
    }
    named::<ConnectedRedisBroker, RedisPubSubPattern>();
}

// The harness cases: the service's app, run in process.

/// The request of the harness cases. It is injected through the publish builder, so it derives
/// `Outgoing`; declaring no name of its own leaves the stream key to the injecting call.
#[derive(Serialize, Deserialize, PartialEq, Debug, Outgoing)]
struct Order {
    id: u64,
}

/// A reply that fixes its own destination: every `Confirmation` this service sends goes to the
/// `confirmations` stream, so the subscriber clause names nothing.
#[derive(Serialize, Deserialize, PartialEq, Debug, Outgoing)]
#[outgoing(name = "confirmations")]
struct Confirmation {
    id: u64,
}

/// A reply that declares no destination: the mount site's `publish("..")` is the one that applies.
#[derive(Serialize, Deserialize, PartialEq, Debug, Outgoing)]
struct Receipt {
    id: u64,
}

/// An event the service announces on a channel its pattern subscription reads.
#[derive(Serialize, Deserialize, PartialEq, Debug, Outgoing)]
#[outgoing(name = "events.eu")]
struct Announced {
    id: u64,
}

#[subscriber(RedisStream::new("orders.confirmed").group("workers"), publish)]
async fn confirm_order(order: &Order) -> Confirmation {
    Confirmation { id: order.id }
}

#[subscriber(RedisStream::new("orders.receipted").group("workers"), publish("receipts"))]
async fn receipt_for_order(order: &Order) -> Receipt {
    Receipt { id: order.id }
}

#[subscriber(RedisStream::new("orders").group("workers"))]
async fn ack_order(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::ack()
}

/// Counts how many times the retry handler ran, so the test can wire it as typed app state.
#[derive(Clone, Default)]
struct Attempts(Arc<AtomicUsize>);

#[subscriber(RedisStream::new("retry").group("workers"))]
async fn retry_then_ack(order: &Order, ctx: &mut Context<'_, (), Attempts>) -> HandlerOutcome {
    let _ = order;
    if ctx.state().0.fetch_add(1, Ordering::SeqCst) == 0 {
        HandlerOutcome::retry()
    } else {
        HandlerOutcome::ack()
    }
}

#[subscriber(RedisList::new("jobs").reliable())]
async fn drain_job(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::ack()
}

#[subscriber(RedisPubSub::new("notifications"))]
async fn note_notification(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::ack()
}

/// Two consumers of one group on `shared`, and a second group on it.
#[subscriber(RedisStream::new("shared").group("workers").consumer("a"))]
async fn shared_a(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::ack()
}

#[subscriber(RedisStream::new("shared").group("workers").consumer("b"))]
async fn shared_b(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::ack()
}

#[subscriber(RedisStream::new("shared").group("audit"))]
async fn shared_audit(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::ack()
}

/// Announces every order on the `events.eu` channel.
#[subscriber(RedisStream::new("orders.announced").group("workers"), publish)]
async fn announce(order: &Order) -> Announced {
    Announced { id: order.id }
}

#[subscriber(RedisPubSubPattern::new("events.*"))]
async fn glob_events(event: &Announced) -> HandlerOutcome {
    let _ = event;
    HandlerOutcome::ack()
}

/// The service's app: what `main` runs, and what the harness cases hand the harness.
fn app() -> impl App {
    RustStream::new(AppInfo::new("svc", "0.1.0"))
        .on_startup(|()| async { Ok::<_, std::convert::Infallible>(Attempts::default()) })
        .with_broker(RedisBroker::standalone(URL), |b| {
            b.include(ack_order);
            b.include(retry_then_ack);
            b.include(confirm_order);
            b.include(receipt_for_order);
            b.include(drain_job);
            b.include(note_notification);
            b.include(shared_a);
            b.include(shared_b);
            b.include(shared_audit);
            b.include(announce).out_reply(RedisPubSubPublish::new());
            b.include(glob_events)
                .out_retry(RedisPubSubPublish::new())
                .to("events.retry");
        })
}

// The harness installs its coordinator into the in-process server, so `publish` drives the
// reaction to quiescence (every delivery counted in flight released) before returning.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_app_drives_the_in_process_server_to_quiescence() {
    let tb = TestApp::start(app()).await.expect("start");

    tb.broker::<RedisBroker>()
        .publish("orders", &Order { id: 1 })
        .await
        .expect("publish must drive the reaction to quiescence");

    tb.broker::<RedisBroker>()
        .subscriber("orders")
        .assert_called_once()
        .with(&Order { id: 1 })
        .settled(HandlerOutcome::ack());

    tb.shutdown().await.expect("shutdown");
}

// A requeue appends a copy of the entry, so the harness still reaches quiescence: the second
// delivery's ack balances the count. The handler is called exactly twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_app_requeue_stays_balanced() {
    let tb = TestApp::start(app()).await.expect("start");

    tb.broker::<RedisBroker>()
        .publish("retry", &Order { id: 7 })
        .await
        .expect("publish must drive the requeue reaction to quiescence");

    tb.broker::<RedisBroker>()
        .subscriber("retry")
        .assert_called(2)
        .settled(HandlerOutcome::ack());
    // The copy is a second entry on the stream, as on a server.
    tb.broker::<RedisBroker>()
        .published::<Order>("retry")
        .assert_called(2);

    tb.shutdown().await.expect("shutdown");
}

// The reply type declares `confirmations`, so the `XADD` goes there and the attribute's clause
// carries no name at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reply_lands_on_the_stream_its_type_declares() {
    let tb = TestApp::start(app()).await.expect("start");

    tb.broker::<RedisBroker>()
        .message(&Order { id: 1 })
        .to("orders.confirmed")
        .publish()
        .await
        .expect("publish");

    tb.broker::<RedisBroker>()
        .subscriber("orders.confirmed")
        .assert_called_once();
    tb.broker::<RedisBroker>()
        .published::<Confirmation>("confirmations")
        .assert_called_once()
        .with(&Confirmation { id: 1 });

    tb.shutdown().await.expect("shutdown");
}

// The reply type declares nothing, so the stream key comes from the mount site's
// `publish("receipts")`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reply_without_a_declared_stream_lands_where_the_mount_site_says() {
    let tb = TestApp::start(app()).await.expect("start");

    tb.broker::<RedisBroker>()
        .message(&Order { id: 2 })
        .to("orders.receipted")
        .publish()
        .await
        .expect("publish");

    tb.broker::<RedisBroker>()
        .subscriber("orders.receipted")
        .assert_called_once();
    tb.broker::<RedisBroker>()
        .published::<Receipt>("receipts")
        .assert_called_once()
        .with(&Receipt { id: 2 });

    tb.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_app_mounts_a_list_descriptor() {
    let tb = TestApp::start(app()).await.expect("start");

    tb.broker::<RedisBroker>()
        .publish("jobs", &Order { id: 3 })
        .await
        .expect("publish");

    tb.broker::<RedisBroker>()
        .subscriber("jobs")
        .assert_called_once()
        .with(&Order { id: 3 })
        .settled(HandlerOutcome::ack());

    tb.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_app_mounts_a_pubsub_descriptor() {
    let tb = TestApp::start(app()).await.expect("start");

    tb.broker::<RedisBroker>()
        .publish("notifications", &Order { id: 5 })
        .await
        .expect("publish");

    tb.broker::<RedisBroker>()
        .subscriber("notifications")
        .assert_called_once()
        .with(&Order { id: 5 })
        .settled(HandlerOutcome::ack());

    tb.shutdown().await.expect("shutdown");
}

// One entry of `shared` reaches one of the two consumers of `workers` and the one of `audit`: two
// deliveries in all, recorded under the stream's name.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_app_delivers_once_per_consumer_group() {
    let tb = TestApp::start(app()).await.expect("start");

    tb.broker::<RedisBroker>()
        .publish("shared", &Order { id: 6 })
        .await
        .expect("publish");

    tb.broker::<RedisBroker>()
        .subscriber("shared")
        .assert_called(2)
        .with(&Order { id: 6 });

    tb.shutdown().await.expect("shutdown");
}

// The announcement is a `PUBLISH` on `events.eu`, which the `events.*` pattern subscription reads.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_app_mounts_a_pattern_descriptor() {
    let tb = TestApp::start(app()).await.expect("start");

    tb.broker::<RedisBroker>()
        .publish("orders.announced", &Order { id: 8 })
        .await
        .expect("publish");

    tb.broker::<RedisBroker>()
        .subscriber("events.*")
        .assert_called_once()
        .with(&Announced { id: 8 });

    tb.shutdown().await.expect("shutdown");
}

// A descriptor a real server would refuse at startup is refused here too, or the harness
// green-lights a service that cannot deploy.

#[subscriber(RedisStream::new("ungrouped"))]
async fn ungrouped(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::ack()
}

#[subscriber(RedisList::new("orphans").recovery_zset("orphans.claims"))]
async fn recover_orphan(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stream_without_a_group_is_refused_in_process_too() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(
        RedisBroker::standalone(URL),
        |b| {
            b.include(ungrouped);
        },
    );
    let err = TestApp::start(app)
        .await
        .expect_err("the subscription must be refused at startup");
    assert!(
        format!("{err}").contains("requires a consumer group"),
        "got {err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_list_recovery_without_min_idle_is_refused_in_process_too() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(
        RedisBroker::standalone(URL),
        |b| {
            b.include(recover_orphan);
        },
    );
    let err = TestApp::start(app)
        .await
        .expect_err("the subscription must be refused at startup");
    assert!(format!("{err}").contains("needs a min_idle"), "got {err}");
}

/// A pattern registration that names no destination for its copies refuses to start, because the
/// descriptor addresses none itself.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pattern_registration_without_a_destination_refuses_to_start() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(
        RedisBroker::standalone(URL),
        |b| {
            b.include(glob_events);
        },
    );
    let err = TestApp::start(app)
        .await
        .expect_err("the registration must be refused at startup");
    assert!(
        format!("{err}").contains("names no destination"),
        "got {err}"
    );
}
