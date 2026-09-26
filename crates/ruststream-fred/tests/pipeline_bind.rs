//! Publishers join a delivery's round: a slot's publisher bound with `pipeline.bind(&out)`, and a
//! reply whose mount names the `InRound` transform. What joins the round follows the delivery's
//! outcome and leaves after what the handler queued before it; an unbound slot publish leaves at
//! once.

#![cfg(feature = "testing")]

use ruststream::testing::TestApp;
use ruststream_fred::PipelinedStream;
use ruststream_fred::context::keys;
use ruststream_fred::pipeline::{Bindable, InRound};
use ruststream_fred::prelude::*;
use serde::{Deserialize, Serialize};

/// The address the service's broker is built with; the in-process mode dials nothing.
const URL: &str = "redis://localhost:6379";

#[derive(Debug, Deserialize, Outgoing, PartialEq, Serialize)]
struct Order {
    id: u64,
    keep: bool,
}

#[derive(Debug, Deserialize, Outgoing, PartialEq, Serialize)]
#[outgoing(name = "audit")]
struct Audit {
    what: String,
}

fn outcome(order: &Order) -> HandlerOutcome {
    if order.keep {
        HandlerOutcome::ack()
    } else {
        HandlerOutcome::drop()
    }
}

#[subscriber(PipelinedStream::new("orders").group("workers"))]
async fn bound(
    order: &Order,
    Ctx(pipeline): Ctx<keys::Pipeline>,
    Out(out): Out<impl Bindable>,
) -> HandlerOutcome {
    let out = pipeline.bind(out);
    let audit = Audit {
        what: "bound".to_owned(),
    };
    if out.message(&audit).publish().await.is_err() {
        return HandlerOutcome::retry();
    }
    outcome(order)
}

#[subscriber(PipelinedStream::new("orders").group("workers"))]
async fn unbound(order: &Order, Out(out): Out<impl Bindable>) -> HandlerOutcome {
    let audit = Audit {
        what: "unbound".to_owned(),
    };
    if out.message(&audit).publish().await.is_err() {
        return HandlerOutcome::retry();
    }
    outcome(order)
}

macro_rules! case {
    ($test:ident, $handler:ident, $keep:literal, $expected:literal) => {
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn $test() {
            let app = RustStream::new(AppInfo::new("pipeline", "0.1.0")).with_broker(
                RedisBroker::standalone(URL),
                |b| {
                    b.include($handler)
                        .out(DefaultSlot, stream::Publish)
                        .build();
                },
            );
            let tb = TestApp::start(app).await.expect("start");

            tb.broker::<RedisBroker>()
                .message(&Order { id: 1, keep: $keep })
                .to("orders")
                .publish()
                .await
                .expect("publish");

            tb.broker::<RedisBroker>()
                .subscriber("orders")
                .assert_called_once();
            tb.broker::<RedisBroker>()
                .published::<Audit>("audit")
                .assert_called($expected);

            tb.shutdown().await.expect("shutdown");
        }
    };
}

case!(a_bound_publish_leaves_with_the_ack, bound, true, 1);
case!(
    a_bound_publish_of_a_dropped_delivery_never_leaves,
    bound,
    false,
    0
);
case!(an_unbound_publish_leaves_at_once, unbound, false, 1);

#[subscriber(PipelinedStream::new("orders").group("workers"), publish)]
async fn replies(order: &Order, Ctx(pipeline): Ctx<keys::Pipeline>) -> Audit {
    let queued = Audit {
        what: "queued".to_owned(),
    };
    let body = serde_json::to_vec(&queued).expect("an audit encodes");
    pipeline
        .xadd("audit", false, None::<()>, "*", vec![("_payload", body)])
        .await
        .expect("queued");
    let _ = order;
    Audit {
        what: "reply".to_owned(),
    }
}

/// What reached the audit stream, in the order it arrived.
fn audit_order(tb: &TestApp<()>) -> Vec<String> {
    tb.broker::<RedisBroker>()
        .published::<Audit>("audit")
        .messages()
        .iter()
        .map(|raw| {
            serde_json::from_slice::<Audit>(raw.payload())
                .expect("an audit decodes")
                .what
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reply_in_the_round_leaves_after_what_the_handler_queued() {
    let app = RustStream::new(AppInfo::new("pipeline", "0.1.0")).with_broker(
        RedisBroker::standalone(URL),
        |b| {
            b.include(replies)
                .out_reply(stream::Publish)
                .transform(InRound);
        },
    );
    let tb = TestApp::start(app).await.expect("start");

    tb.broker::<RedisBroker>()
        .message(&Order { id: 2, keep: true })
        .to("orders")
        .publish()
        .await
        .expect("publish");

    assert_eq!(audit_order(&tb), ["queued", "reply"]);

    tb.shutdown().await.expect("shutdown");
}

/// Without the transform a reply is an ordinary publish: it leaves as the handler returns, before
/// the window sends what the handler queued.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reply_outside_the_round_leaves_before_the_window() {
    let app = RustStream::new(AppInfo::new("pipeline", "0.1.0")).with_broker(
        RedisBroker::standalone(URL),
        |b| {
            b.include(replies).out_reply(stream::Publish);
        },
    );
    let tb = TestApp::start(app).await.expect("start");

    tb.broker::<RedisBroker>()
        .message(&Order { id: 3, keep: true })
        .to("orders")
        .publish()
        .await
        .expect("publish");

    assert_eq!(audit_order(&tb), ["reply", "queued"]);

    tb.shutdown().await.expect("shutdown");
}
