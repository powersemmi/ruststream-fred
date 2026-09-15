//! The partition key as a per-message option, driven through the application harness.
//!
//! What a service author writes is a step on the publish builder, and the assertions here read it
//! back from both ends: the slot view holds the options the publish carried, and a consumer
//! handler mounted on the destination reports the header they resolved into. The mount sites are
//! the ones a routes file writes, so nothing here names a test-only type.

#![cfg(feature = "testing")]

use ruststream::codec::CborCodec;
use ruststream::testing::TestApp;
use ruststream_fred::stream::prelude::*;
use ruststream_fred::testing::RedisTestBroker;
use serde::{Deserialize, Serialize};

/// The message the handlers forward. It names no destination, so every publish picks one.
#[derive(Debug, Deserialize, Outgoing, PartialEq, Serialize)]
struct Order {
    id: u64,
}

/// What the consumer end saw: the partition key its delivery carried, empty when it carried none.
#[derive(Debug, Deserialize, Outgoing, PartialEq, Serialize)]
#[outgoing(name = "orders.seen")]
struct Seen {
    key: String,
}

#[derive(OutSlot)]
#[publishes(Order)]
struct Ledger;

#[derive(OutSlot)]
#[publishes(Order)]
struct Notes;

/// A body that adjusts a per-message setting names this crate's step, so it imports this crate's
/// prelude and its bound names the options type. That is the one exception to a handler body
/// importing the framework prelude alone, and the signature says which broker it is tied to.
#[subscriber(RedisStream::new("orders.in").group("workers"))]
async fn forward(
    order: &Order,
    Out(ledger): Out<impl Publisher<Options = RedisPublishOptions>, Ledger>,
) -> HandlerOutcome {
    if ledger
        .message(order)
        .to("orders.keyed")
        .partition_key("tenant-a")
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

/// The same publish without the step: nothing is set, and the policy's own settings are the whole
/// answer.
#[subscriber(RedisStream::new("orders.plain.in").group("workers"))]
async fn forward_unkeyed(
    order: &Order,
    Out(notes): Out<impl Publisher<Options = RedisPublishOptions>, Notes>,
) -> HandlerOutcome {
    if notes
        .message(order)
        .to("orders.plain")
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

/// The consumer end of the keyed publish: it reports the key its own delivery carried, which is
/// where the runtime's `workers(n, by_key)` lanes read it from.
#[subscriber(RedisStream::new("orders.keyed").group("watchers"), publish)]
async fn watch_keyed(order: &Order, ctx: &mut Context<'_>) -> Seen {
    let _ = order;
    Seen { key: seen_key(ctx) }
}

/// The same, for the destination nothing keyed.
#[subscriber(RedisStream::new("orders.plain").group("watchers"), publish)]
async fn watch_unkeyed(order: &Order, ctx: &mut Context<'_>) -> Seen {
    let _ = order;
    Seen { key: seen_key(ctx) }
}

/// The delivery's partition key as text, empty when the delivery carries none.
fn seen_key<Kind, AppState>(ctx: &Context<'_, Kind, AppState>) -> String {
    ctx.headers()
        .get(PARTITION_KEY_HEADER)
        .map(|key| String::from_utf8_lossy(key).into_owned())
        .unwrap_or_default()
}

/// The step is the whole call site: the slot view holds the options it set, and the consumer end
/// reports the key those options resolved into.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_step_sets_the_key_the_delivery_reports() {
    let app =
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(RedisTestBroker::new(), |b| {
            b.include(forward).out(Ledger, Publish).build();
            b.include(watch_keyed).out_reply(Publish);
        });
    let tb = TestApp::start(app).await.expect("start");

    tb.broker::<RedisTestBroker>()
        .publish("orders.in", &Order { id: 7 })
        .await
        .expect("publish");

    tb.out::<Ledger>()
        .assert_called_once()
        .with_options(&RedisPublishOptions {
            partition_key: Some(b"tenant-a".to_vec()),
        });
    tb.broker::<RedisTestBroker>()
        .published::<Seen>("orders.seen")
        .assert_called_once()
        .with(&Seen {
            key: "tenant-a".to_owned(),
        });

    tb.shutdown().await.expect("shutdown");
}

/// A publish no step touched carries no options at all, and nothing is written into its headers:
/// a key is per message by nature, so there is no publisher-wide default to inherit.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_publish_without_the_step_carries_no_key() {
    let app =
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(RedisTestBroker::new(), |b| {
            b.include(forward_unkeyed).out(Notes, Publish).build();
            b.include(watch_unkeyed).out_reply(Publish);
        });
    let tb = TestApp::start(app).await.expect("start");

    tb.broker::<RedisTestBroker>()
        .publish("orders.plain.in", &Order { id: 7 })
        .await
        .expect("publish");

    tb.out::<Notes>()
        .assert_called_once()
        .assert_options_default();
    tb.broker::<RedisTestBroker>()
        .published::<Seen>("orders.seen")
        .assert_called_once()
        .with(&Seen { key: String::new() });

    tb.shutdown().await.expect("shutdown");
}

/// The defect the option closes. A step is a position on the builder rather than a wrapper around
/// the publisher, so the keyed publish still leaves through the mount site's own entry, encoded
/// with the codec that entry named. The adapter this replaced lost both.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_keyed_publish_keeps_the_codec_the_mount_site_named() {
    let app =
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(RedisTestBroker::new(), |b| {
            b.include(forward)
                .out(Ledger, Publish)
                .codec(CborCodec)
                .build();
        });
    let tb = TestApp::start(app).await.expect("start");

    tb.broker::<RedisTestBroker>()
        .publish("orders.in", &Order { id: 7 })
        .await
        .expect("publish");

    tb.out::<Ledger>()
        .assert_called_once()
        .decoded_as::<Order>()
        .with_codec(&CborCodec, &Order { id: 7 })
        .with_options(&RedisPublishOptions {
            partition_key: Some(b"tenant-a".to_vec()),
        });

    tb.shutdown().await.expect("shutdown");
}
