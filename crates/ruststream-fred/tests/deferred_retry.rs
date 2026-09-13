//! The deferred retry a subscription without a delay queue gets from the runtime.
//!
//! Redis Streams have no per-message delay, so `retry_after` is served by a copy the runtime
//! publishes back to the stream once the delay is up. The mount site names the publisher that copy
//! leaves through, and the position is an ordinary `Out` slot: the transform mounted on it here
//! stamps the copy, and the stamp is what the redelivered message carries.

#![cfg(feature = "testing")]

use std::time::Duration;

use ruststream::runtime::{Outgoing, RETRY_COUNT_HEADER, SlotContext};
use ruststream::testing::{Outcome, TestApp};
use ruststream_fred::stream::prelude::*;
use ruststream_fred::testing::RedisTestBroker;
use serde::{Deserialize, Serialize};

const RETRY_DELAY: Duration = Duration::from_secs(30);

/// The header the transform writes on every copy leaving the retry position.
const LEFT_THROUGH: &str = "x-left-through";

#[derive(Debug, Deserialize, PartialEq, Serialize)]
struct Order {
    id: u64,
}

/// Stamps the slot a message left through, so a deferred copy is recognisable downstream. The
/// retry position offers a slot's own view, and the transform writes no broker setting, so it is
/// generic over the options type and mounts on any publisher.
struct DeferredStamp;

impl<Options> PublishTransform<ForSlot, Options> for DeferredStamp {
    type Destination = Reads;

    fn apply(&self, out: &mut Outgoing<'_>, _options: &mut Option<Options>, cx: &SlotContext<'_>) {
        out.headers_mut().insert(LEFT_THROUGH, cx.slot().to_owned());
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
        .with_header(LEFT_THROUGH, "Retry");
    assert_eq!(
        tb.broker::<RedisTestBroker>()
            .subscriber("billing")
            .outcomes(),
        [Outcome::Nack, Outcome::Ack],
        "the deferred copy must reach the handler and settle",
    );

    tb.shutdown().await.expect("shutdown");
}
