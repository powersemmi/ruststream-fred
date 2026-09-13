//! The deferred retry a subscription without a delay queue gets from the runtime, and the delay
//! queue that replaces it.
//!
//! A plain Redis Streams subscription has no per-message delay, so `retry_after` is served by a
//! copy the runtime publishes back to the stream once the delay is up. The mount site names the
//! publisher that copy leaves through, and the position is an ordinary `Out` slot: the transform
//! mounted on it here stamps the copy, and the stamp is what the redelivered message carries.
//!
//! Naming a ZSET delay queue with `delayed_retry` takes the delay to Redis instead, and then
//! nothing is published: the entry itself comes back.

#![cfg(feature = "testing")]

use std::time::Duration;

use ruststream::runtime::{Outgoing, PublishContext, RETRY_COUNT_HEADER};
use ruststream::testing::{Outcome, TestApp};
use ruststream_fred::stream::prelude::*;
use ruststream_fred::testing::RedisTestBroker;
use serde::{Deserialize, Serialize};

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

/// The copy the runtime publishes travels the retry position's pipeline, so the transform mounted
/// there stamps it, and it reaches the handler again through the address the subscription reports.
#[tokio::test(start_paused = true)]
async fn a_transform_on_the_retry_position_stamps_the_deferred_copy() {
    let app = RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(
        RedisTestBroker::new(),
        |b| {
            b.include(bill_order)
                .out_retry(Publish)
                .transform(DeferredStamp);
        },
    );
    let tb = TestApp::start(app).await.expect("start");

    tb.broker::<RedisTestBroker>()
        .publish("billing", &Order { id: 7 })
        .await
        .expect("publish");
    tb.broker::<RedisTestBroker>()
        .subscriber("billing")
        .assert_called_once()
        .settled(HandlerOutcome::retry_after(RETRY_DELAY));

    tb.advance(RETRY_DELAY).await.expect("advance");

    tb.broker::<RedisTestBroker>()
        .published::<Order>("billing")
        .with_header(LEFT_THROUGH, "billing");
    assert_eq!(
        tb.broker::<RedisTestBroker>()
            .subscriber("billing")
            .outcomes(),
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

/// A delay queue serves the delay itself, so the entry comes back after it with nothing published
/// in between: one message on the stream, the one the test put there.
#[tokio::test(start_paused = true)]
async fn a_delay_queue_holds_the_message_without_publishing_a_copy() {
    let app = RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(
        RedisTestBroker::new(),
        |b| {
            b.include(invoice_order);
        },
    );
    let tb = TestApp::start(app).await.expect("start");

    tb.broker::<RedisTestBroker>()
        .publish("invoices", &Order { id: 9 })
        .await
        .expect("publish");
    tb.broker::<RedisTestBroker>()
        .subscriber("invoices")
        .assert_called_once()
        .settled(HandlerOutcome::retry_after(RETRY_DELAY));

    // Half the delay is not the delay: the message is still parked.
    tb.advance(RETRY_DELAY / 2).await.expect("advance");
    tb.broker::<RedisTestBroker>()
        .subscriber("invoices")
        .assert_called(1);

    tb.advance(RETRY_DELAY).await.expect("advance");
    tb.broker::<RedisTestBroker>()
        .subscriber("invoices")
        .assert_called(2);
    // The copy path would have written a second entry here, carrying the retry-count header.
    tb.broker::<RedisTestBroker>()
        .published::<Order>("invoices")
        .assert_called_once();

    tb.shutdown().await.expect("shutdown");
}
