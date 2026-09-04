//! Integration tests for the in-process Redis test broker.
//!
//! Most cases drive the public surface (`RedisTestBroker`, `RedisTestPublisher`,
//! `RedisTestSubscriber`) directly, to keep failures localised; the `TestApp`-driven cases at the
//! end exercise the `TestableBroker` quiescence wiring (coordinator install, `enqueued`/`consumed`)
//! through the harness. Real consumer-group semantics live in `tests/integration_fred.rs` against a
//! live Redis server.

#![cfg(feature = "testing")]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures::{Stream, StreamExt};
use ruststream::runtime::{AppInfo, HandlerOutcome, PublishExt, RustStream};
use ruststream::subscriber;
use ruststream::testing::TestApp;
use ruststream::{
    BatchSubscriber, Broker, ConnectedBroker, DescribeServer, HeaderMap, IncomingMessage, Outgoing,
    OutgoingMessage, OwnedTransactions, Partitioned, Publisher, RawMessage, Serialized, Subscriber,
    Transaction, TransactionalPublisher, nonzero, testing::expect_published,
};
use ruststream_fred::{
    PARTITION_KEY_HEADER, RedisError, RedisPublishExt, RedisStream,
    testing::{ConnectedRedisTestBroker, RedisTestBroker, RedisTestMessage},
};
use serde::{Deserialize, Serialize};

const WAIT: Duration = Duration::from_secs(1);

/// A freshly connected in-process broker: the form that carries the subscribe, publish, and
/// `TestableBroker` surface.
async fn connected() -> ConnectedRedisTestBroker {
    RedisTestBroker::new().connect().await.expect("connect")
}

async fn next_payload<S>(stream: &mut S) -> Vec<u8>
where
    S: Stream<Item = Result<RedisTestMessage, RedisError>> + Unpin,
{
    let msg = tokio::time::timeout(WAIT, stream.next())
        .await
        .expect("delivery within timeout")
        .expect("stream has next")
        .expect("delivery ok");
    let payload = msg.payload().to_vec();
    msg.ack().await.expect("ack");
    payload
}

/// The undrained counterpart of [`next_payload`], for cases that assert on headers before settling.
async fn next_message<S>(stream: &mut S) -> RedisTestMessage
where
    S: Stream<Item = Result<RedisTestMessage, RedisError>> + Unpin,
{
    tokio::time::timeout(WAIT, stream.next())
        .await
        .expect("delivery within timeout")
        .expect("stream has next")
        .expect("delivery ok")
}

/// A message declaring a header contract: the shape whose publish leaves the builder's headers
/// position occupied, so the partition key has to ride elsewhere.
#[derive(Outgoing, Serialize, Deserialize)]
#[outgoing(name = "orders.keyed", headers = OrderMeta)]
struct KeyedOrder {
    id: u64,
}

#[derive(Serialize, Deserialize)]
struct OrderMeta {
    region: String,
}

/// An opaque payload for the partition-key cases: they assert on the header the keyed handle
/// contributes, not on what a codec would make of the body, so the bytes leave as they are.
#[derive(Outgoing, Serialized)]
struct Payload(Vec<u8>);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pub_sub_round_trip_through_broker_traits() {
    let broker = connected().await;

    let mut subscriber = broker.subscribe("orders").await.expect("subscribe");
    let publisher = broker.publisher();

    publisher
        .publish(OutgoingMessage::new("orders", b"o1"))
        .await
        .expect("publish");

    let mut stream = Box::pin(subscriber.stream());
    let got = next_payload(&mut stream).await;
    assert_eq!(got, b"o1");
    drop(stream);

    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publisher_rejects_empty_key() {
    let broker = connected().await;
    let publisher = broker.publisher();
    let err = publisher
        .publish(OutgoingMessage::new("", b"x"))
        .await
        .expect_err("empty key must be rejected");
    assert!(format!("{err}").contains("publish"), "got {err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn distinct_keys_are_isolated() {
    let broker = connected().await;
    let mut orders = broker.subscribe("orders").await.expect("subscribe orders");
    let mut events = broker.subscribe("events").await.expect("subscribe events");
    let publisher = broker.publisher();

    publisher
        .publish(OutgoingMessage::new("orders", b"o"))
        .await
        .expect("publish o");
    publisher
        .publish(OutgoingMessage::new("events", b"e"))
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
    let mut subscriber = broker.subscribe("orders").await.expect("subscribe");
    let publisher = broker.publisher();

    publisher
        .publish(OutgoingMessage::new("orders", b"once"))
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
    let mut subscriber = broker.subscribe("orders").await.expect("subscribe");
    let publisher = broker.publisher();

    let mut headers = HeaderMap::new();
    headers.insert("content-type", "application/json");
    headers.insert("correlation-id", "abc-1");
    let outgoing = OutgoingMessage::new("orders", b"{}").with_headers(headers);
    publisher.publish(outgoing).await.expect("publish");

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
        .publish(OutgoingMessage::new("events", b"first"))
        .await
        .expect("publish first");
    publisher
        .publish(OutgoingMessage::new("events", b"second"))
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
    let mut subscriber = broker.subscribe("orders").await.expect("subscribe");
    let publisher = broker.publisher();

    publisher
        .publish(OutgoingMessage::new("orders", b"one"))
        .await
        .expect("publish one");
    {
        let mut stream = Box::pin(subscriber.stream());
        assert_eq!(next_payload(&mut stream).await, b"one");
    }

    publisher
        .publish(OutgoingMessage::new("orders", b"two"))
        .await
        .expect("publish two");
    let mut stream = Box::pin(subscriber.stream());
    assert_eq!(next_payload(&mut stream).await, b"two");
}

#[tokio::test]
async fn describe_server_returns_redis_protocol() {
    // `DescribeServer` describes the configuration, so it sits on the unconnected form.
    let spec = RedisTestBroker::new().describe_server();
    assert_eq!(spec.protocol, "redis");
}

#[tokio::test]
async fn partition_key_header_is_surfaced() {
    let broker = connected().await;
    let mut sub = broker.subscribe("events").await.expect("subscribe");

    let mut headers = HeaderMap::new();
    headers.insert(PARTITION_KEY_HEADER, "tenant-a");

    broker
        .publisher()
        .publish(OutgoingMessage::new("events", b"payload").with_headers(headers))
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

#[tokio::test]
async fn partition_key_absent_yields_none() {
    let broker = connected().await;
    let mut sub = broker.subscribe("events.bare").await.expect("subscribe");

    broker
        .publisher()
        .publish(OutgoingMessage::new("events.bare", b"payload"))
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

/// The adapter is the publish-side counterpart of `Partitioned`: what it carries is what the
/// delivery reports, with no hand-built header map at the call site.
#[tokio::test]
async fn partition_key_step_carries_the_header() {
    let broker = connected().await;
    let mut sub = broker.subscribe("keyed.plain").await.expect("subscribe");
    let publisher = broker.publisher();

    publisher
        .partition_key("tenant-a")
        .message(&Payload(b"payload".to_vec()))
        .to("keyed.plain")
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
/// headers position with that contract, so a partition key has nowhere else to go. Travelling
/// beneath it as base headers, the key composes with the contract instead of competing for it.
#[tokio::test]
async fn partition_key_step_composes_with_a_header_contract() {
    let broker = connected().await;
    let mut sub = broker.subscribe("orders.keyed").await.expect("subscribe");
    let publisher = broker.publisher();

    publisher
        .partition_key("tenant-a")
        .message(&KeyedOrder { id: 7 })
        .with_headers(&OrderMeta {
            region: "eu".into(),
        })
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

/// The handle's key sits under the call's headers, not over them, so naming unrelated headers at
/// the call site leaves the key in place and the call's own entries untouched.
#[tokio::test]
async fn partition_key_step_survives_unrelated_call_site_headers() {
    let broker = connected().await;
    let mut sub = broker.subscribe("keyed.map").await.expect("subscribe");
    let publisher = broker.publisher();

    let mut headers = HeaderMap::new();
    headers.insert("trace-id", "abc");

    publisher
        .partition_key("tenant-b")
        .message(&Payload(b"payload".to_vec()))
        .with_headers(headers)
        .to("keyed.map")
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

/// Call site wins: the handle serves many publishes, a call names one message, so a partition key
/// written into the publish's own headers overrides the one the handle carries.
#[tokio::test]
async fn call_site_partition_key_overrides_the_step() {
    let broker = connected().await;
    let mut sub = broker.subscribe("keyed.override").await.expect("subscribe");
    let publisher = broker.publisher();

    let mut headers = HeaderMap::new();
    headers.insert(PARTITION_KEY_HEADER, "call-site");

    publisher
        .partition_key("handle")
        .message(&Payload(b"payload".to_vec()))
        .with_headers(headers)
        .to("keyed.override")
        .publish()
        .await
        .expect("publish");

    let msg = next_message(&mut Box::pin(sub.stream())).await;
    assert_eq!(
        Partitioned::partition_key(&msg),
        Some(b"call-site".as_slice())
    );
    msg.ack().await.ok();
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn batch_drains_in_publish_order() {
    let broker = connected().await;
    let publisher = broker.publisher();
    let mut sub = broker.subscribe("batch.order").await.expect("subscribe");

    let count = 5u8;
    for i in 0..count {
        publisher
            .publish(OutgoingMessage::new("batch.order", &[i]))
            .await
            .expect("publish");
    }

    // Opened smaller than the run, so the assertion below reads the contract rather than the
    // publish count: a page never carries more than the size the subscription named.
    let mut batches = Box::pin(sub.batches(nonzero!(3)));
    let batch = tokio::time::timeout(WAIT, batches.next())
        .await
        .expect("batch within timeout")
        .expect("stream has next")
        .expect("ok batch");

    assert!(!batch.is_empty(), "batch must contain at least one message");
    assert!(batch.len() <= 3, "a page must not exceed its size");
    for (i, msg) in batch.into_iter().enumerate() {
        assert_eq!(msg.payload(), &[u8::try_from(i).expect("count fits u8")]);
        msg.ack().await.ok();
    }
    broker.shutdown().await.expect("shutdown");
}

// Same re-entry contract as `stream()`: dropping the batch stream and calling `batches()` again
// must keep working.
#[tokio::test]
async fn batches_can_be_reentered() {
    let broker = connected().await;
    let publisher = broker.publisher();
    let mut sub = broker.subscribe("batch.reenter").await.expect("subscribe");

    publisher
        .publish(OutgoingMessage::new("batch.reenter", b"one"))
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
        .publish(OutgoingMessage::new("batch.reenter", b"two"))
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
    let mut sub = broker.subscribe("tx").await.expect("subscribe");
    let publisher = broker.publisher();

    publisher.begin_transaction().await.expect("begin");
    publisher
        .publish(OutgoingMessage::new("tx", b"first"))
        .await
        .expect("publish first");
    publisher
        .publish(OutgoingMessage::new("tx", b"second"))
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
        .publish(OutgoingMessage::new("tx", b"discarded"))
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
        .publish(OutgoingMessage::new("tx.misuse", b"kept"))
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
        .publish(OutgoingMessage::new("owned.orders", b"o1"))
        .await
        .expect("buffer o1");
    orders
        .publish(OutgoingMessage::new("owned.orders", b"o2"))
        .await
        .expect("buffer o2");
    audit
        .publish(OutgoingMessage::new("owned.audit", b"a1"))
        .await
        .expect("buffer a1");

    // A direct publish through the same handle is unaffected by the open transactions.
    publisher
        .publish(OutgoingMessage::new("owned.orders", b"direct"))
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
    txn.publish(OutgoingMessage::new("owned.abort", b"discarded"))
        .await
        .expect("buffer");
    txn.abort().await.expect("abort");

    let observed = expect_published(&broker, "owned.abort", 1, Duration::from_millis(50)).await;
    assert!(observed.is_empty(), "aborted messages must be discarded");
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct Order {
    id: u64,
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
    // Requeue once, then acknowledge: exercises the `nack(requeue = true)` -> `enqueued` re-count
    // balanced against the delivery's `Drop` -> `consumed` decrement.
    if ctx.state().0.fetch_add(1, Ordering::SeqCst) == 0 {
        HandlerOutcome::retry()
    } else {
        HandlerOutcome::ack()
    }
}

// The harness installs its coordinator into `RedisTestBroker`, so `publish` must drive the
// in-process reaction to quiescence (every `enqueued` balanced by a `consumed`) before returning.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_app_drives_redis_test_broker_to_quiescence() {
    let app =
        RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(RedisTestBroker::new(), |b| {
            b.include(ack_order);
        });
    let tb = TestApp::start(app).await.expect("start");

    tb.broker::<RedisTestBroker>()
        .publish("orders", &Order { id: 1 })
        .await
        .expect("publish must drive the reaction to quiescence");

    tb.broker::<RedisTestBroker>()
        .subscriber("orders")
        .assert_called_once()
        .with(&Order { id: 1 })
        .settled(HandlerOutcome::ack());

    tb.shutdown().await.expect("shutdown");
}

// A requeue re-enqueues a fresh delivery, so the harness must still reach quiescence: the second
// delivery's ack balances the count. The handler is called exactly twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_app_requeue_stays_balanced() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .on_startup(|()| async { Ok::<_, std::convert::Infallible>(Attempts::default()) })
        .with_broker(RedisTestBroker::new(), |b| {
            b.include(retry_then_ack);
        });
    let tb = TestApp::start(app).await.expect("start");

    tb.broker::<RedisTestBroker>()
        .publish("retry", &Order { id: 7 })
        .await
        .expect("publish must drive the requeue reaction to quiescence");

    tb.broker::<RedisTestBroker>()
        .subscriber("retry")
        .assert_called(2)
        .settled(HandlerOutcome::ack());

    tb.shutdown().await.expect("shutdown");
}
