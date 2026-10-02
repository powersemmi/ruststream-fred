//! The deferred retry a subscription without a delay queue gets from the runtime, and the delay
//! queue that replaces it.
//!
//! A plain Redis Streams subscription has no per-message delay, so `retry_after` is served by a
//! copy the runtime publishes back to the stream once the delay is up. The mount site names the
//! publisher that copy leaves through, and the position is an ordinary `Out` slot: the transform
//! mounted on it here stamps the copy, and the stamp is what the redelivered message carries.
//!
//! Naming a ZSET delay queue with `delayed_retry` takes the delay to Redis instead, and then
//! nothing is published: the entry itself comes back. The entry carries the framework's
//! retry-count header, so a cap declared at the mount site still counts the rounds it makes.

#![cfg(feature = "testing")]

use std::time::Duration;

use ruststream::runtime::{Outgoing, PublishContext, RETRY_COUNT_HEADER};
use ruststream::testing::{Outcome, TestApp};
use ruststream_fred::stream::prelude::*;
use serde::{Deserialize, Serialize};

/// The address the service's broker is built with; the in-process mode dials nothing.
const URL: &str = "redis://localhost:6379";

const RETRY_DELAY: Duration = Duration::from_secs(30);

/// The header the transform writes on every copy leaving the retry position.
const LEFT_THROUGH: &str = "x-retried-from";

#[derive(Debug, Deserialize, PartialEq, Serialize)]
struct Order {
    id: u64,
}

/// Stamps every copy with the subscription the delivery came from, so a deferred copy is
/// recognisable downstream. The retry position reads the delivery being retried, the way a reply's
/// transforms do, and this one writes no broker setting, so it is generic over the options type
/// and mounts on any publisher.
struct DeferredStamp;

impl<C, Options> PublishTransform<ForReply<C>, Options> for DeferredStamp {
    type Destination = Reads;

    fn apply(
        &self,
        out: &mut Outgoing<'_>,
        _options: &mut Option<Options>,
        cx: &PublishContext<'_, C>,
    ) {
        out.headers_mut().insert(LEFT_THROUGH, cx.name().to_owned());
    }
}

/// Defers the first delivery and acknowledges the copy that comes back, so one run covers both
/// ends of the deferral.
#[subscriber(RedisStream::new("billing").group("workers"))]
async fn bill_order(order: &Order, ctx: &mut Context<'_>) -> HandlerOutcome {
    let redelivered = ctx.headers().get(RETRY_COUNT_HEADER).is_some();
    if redelivered {
        assert_eq!(order.id, 7);
        HandlerOutcome::ack()
    } else {
        HandlerOutcome::retry_after(RETRY_DELAY)
    }
}

/// The service's app: what `main` runs, and what every case hands the harness.
fn app() -> impl App<State = ()> {
    RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(
        RedisBroker::standalone(URL),
        |b| {
            b.include(bill_order)
                .out_retry(Publish)
                .transform(DeferredStamp);
            b.include(invoice_order);
            b.include(file_receipt)
                .max_attempts(nonzero!(2u32))
                .dead_letter("receipts.dlq");
        },
    )
}

/// The copy the runtime publishes travels the retry position's pipeline, so the transform mounted
/// there stamps it, and it reaches the handler again through the address the subscription reports.
#[tokio::test(start_paused = true)]
async fn a_transform_on_the_retry_position_stamps_the_deferred_copy() {
    let tb = TestApp::start(app()).await.expect("start");

    tb.broker::<RedisBroker>()
        .publish("billing", &Order { id: 7 })
        .await
        .expect("publish");
    tb.broker::<RedisBroker>()
        .subscriber("billing")
        .assert_called_once()
        .settled(HandlerOutcome::retry_after(RETRY_DELAY));

    tb.advance(RETRY_DELAY).await.expect("advance");

    tb.broker::<RedisBroker>()
        .published::<Order>("billing")
        .with_header(LEFT_THROUGH, "billing");
    assert_eq!(
        tb.broker::<RedisBroker>().subscriber("billing").outcomes(),
        [Outcome::Nack, Outcome::Ack],
        "the deferred copy must reach the handler and settle",
    );

    tb.shutdown().await.expect("shutdown");
}

/// Parks every delivery, so what the test reads is where the delay was served and what, if
/// anything, was written to the stream while it ran.
#[subscriber(
    RedisStream::new("invoices")
        .group("workers")
        .delayed_retry(DelayedRetry::DurableZset { key: "invoices.delayed".to_owned(), ttl: None })
)]
async fn invoice_order(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::retry_after(RETRY_DELAY)
}

/// A delay queue serves the delay itself: nothing reaches the stream while the message is parked,
/// and once the delay has passed the queue's sweep adds it back as a new entry, its retry count
/// raised.
#[tokio::test(start_paused = true)]
async fn a_delay_queue_holds_the_message_without_publishing_a_copy() {
    let tb = TestApp::start(app()).await.expect("start");

    tb.broker::<RedisBroker>()
        .publish("invoices", &Order { id: 9 })
        .await
        .expect("publish");
    tb.broker::<RedisBroker>()
        .subscriber("invoices")
        .assert_called_once()
        .settled(HandlerOutcome::retry_after(RETRY_DELAY));

    // Half the delay is not the delay: the message is still parked, in the queue and not on the
    // stream.
    tb.advance(RETRY_DELAY / 2).await.expect("advance");
    tb.broker::<RedisBroker>()
        .subscriber("invoices")
        .assert_called(1);
    tb.broker::<RedisBroker>()
        .published::<Order>("invoices")
        .assert_called_once();

    tb.advance(RETRY_DELAY).await.expect("advance");
    tb.broker::<RedisBroker>()
        .subscriber("invoices")
        .assert_called(2);
    tb.broker::<RedisBroker>()
        .published::<Order>("invoices")
        .assert_called(2)
        .with_header(RETRY_COUNT_HEADER, "1");

    tb.shutdown().await.expect("shutdown");
}

/// Parks every delivery under a cap, so what the run reads is where the cap stopped it.
#[subscriber(
    RedisStream::new("receipts")
        .group("workers")
        .delayed_retry(DelayedRetry::DurableZset { key: "receipts.delayed".to_owned(), ttl: None })
)]
async fn file_receipt(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::retry_after(RETRY_DELAY)
}

/// A delay queue replays the entry with the retry-count header raised, and that header is the only
/// count a fresh-tail stream subscription has, so a cap declared at the mount site counts the
/// rounds the queue makes and the spent delivery leaves for the dead-letter destination.
#[tokio::test(start_paused = true)]
async fn a_cap_counts_the_rounds_a_delay_queue_replays() {
    let tb = TestApp::start(app()).await.expect("start");

    tb.broker::<RedisBroker>()
        .publish("receipts", &Order { id: 11 })
        .await
        .expect("publish");
    tb.advance(RETRY_DELAY).await.expect("advance");

    tb.broker::<RedisBroker>()
        .subscriber("receipts")
        .assert_called(2);
    tb.broker::<RedisBroker>()
        .published::<Order>("receipts.dlq")
        .assert_called_once();

    tb.shutdown().await.expect("shutdown");
}
