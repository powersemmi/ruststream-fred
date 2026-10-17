//! Services under `TestApp` whose handlers are mounted by hand: a `Handle` body bound to a Redis
//! descriptor with `subscriber(source, body)`, the manual twin of `#[subscriber(..)]`.

#![cfg(feature = "testing")]

use std::future::{Future, ready};
use std::time::Duration;

use ruststream::runtime::{Context, Handle, subscriber};
use ruststream::testing::TestApp;
use ruststream_fred::context::{PipelineContext, keys};
use ruststream_fred::prelude::*;
use ruststream_fred::pubsub::Publish;
use serde::{Deserialize, Serialize};

/// The address the service's broker is built with; the in-process mode dials nothing.
const URL: &str = "redis://localhost:6379";

#[derive(Debug, Deserialize, Outgoing, PartialEq, Serialize)]
struct Order {
    id: u64,
}

/// Takes every order it is handed.
struct TakeOrder;

impl Handle<Order> for TakeOrder {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        let _ = order.id;
        ready(Ok(()))
    }
}

/// Queues an audit entry in the delivery's pipeline, which leaves with its acknowledgement.
struct AuditOrder;

impl Handle<Order, (), (), PipelineContext> for AuditOrder {
    async fn handle(
        &self,
        order: &Order,
        _outs: &(),
        ctx: &mut Context<'_, PipelineContext>,
    ) -> Result<(), HandlerOutcome> {
        let body = serde_json::to_vec(order).expect("an order encodes");
        ctx.context(keys::Pipeline)
            .lpush("audit", body)
            .await
            .map_err(|_| HandlerOutcome::retry())?;
        Ok(())
    }
}

/// Each descriptor mounts as it is, the way the attribute takes it, and its subscription reads
/// what is published to its key or channel. A pattern addresses no retry copy of its own, so its
/// mount site names where one goes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_handle_body_mounts_on_each_redis_descriptor() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(
        RedisBroker::standalone(URL),
        |b| {
            b.include(
                subscriber(RedisStream::new("orders").group("workers"), TakeOrder)
                    .undocumented()
                    .build()
                    .block(Duration::from_millis(50))
                    .start_at(RedisGroupPosition::beginning()),
            );
            b.include(
                subscriber(RedisList::new("jobs"), TakeOrder)
                    .undocumented()
                    .build()
                    .block(Duration::from_millis(50)),
            );
            b.include(
                subscriber(RedisPubSub::new("events"), TakeOrder)
                    .undocumented()
                    .build(),
            );
            b.include(
                subscriber(RedisPubSubPattern::new("refunds.*"), TakeOrder)
                    .undocumented()
                    .build(),
            )
            .out_retry(Publish::default())
            .to("refunds.retry");
        },
    );
    let tb = TestApp::start(app).await.expect("start");

    for (to, id) in [("orders", 1), ("jobs", 2), ("events", 3), ("refunds.eu", 4)] {
        tb.broker::<RedisBroker>()
            .message(&Order { id })
            .to(to)
            .publish()
            .await
            .expect("publish");
    }

    for (subscription, id) in [("orders", 1), ("jobs", 2), ("events", 3), ("refunds.*", 4)] {
        tb.broker::<RedisBroker>()
            .subscriber(subscription)
            .assert_called_once()
            .with(&Order { id })
            .settled(HandlerOutcome::ack());
    }
    tb.shutdown().await.expect("shutdown");
}

/// A descriptor mounted by hand opens its window with `.pipeline()`, and `.atomic()` after it,
/// as an attribute-named one does: what the body queued leaves with the acknowledgement.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_handle_body_on_a_descriptor_opens_its_pipeline() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(
        RedisBroker::standalone(URL),
        |b| {
            b.include(
                subscriber(RedisStream::new("{orders}").group("workers"), AuditOrder)
                    .undocumented()
                    .build()
                    .pipeline()
                    .atomic()
                    .block(Duration::from_millis(50)),
            );
            b.include(
                subscriber(RedisList::new("jobs").reliable(), AuditOrder)
                    .undocumented()
                    .build()
                    .pipeline(),
            );
            b.include(
                subscriber(RedisPubSub::new("events"), AuditOrder)
                    .undocumented()
                    .build()
                    .pipeline(),
            );
            b.include(
                subscriber(RedisPubSubPattern::new("refunds.*"), AuditOrder)
                    .undocumented()
                    .build()
                    .pipeline()
                    .atomic(),
            )
            .out_retry(Publish::default())
            .to("refunds.retry");
        },
    );
    let tb = TestApp::start(app).await.expect("start");

    for (to, id) in [
        ("{orders}", 1),
        ("jobs", 2),
        ("events", 3),
        ("refunds.eu", 4),
    ] {
        tb.broker::<RedisBroker>()
            .message(&Order { id })
            .to(to)
            .publish()
            .await
            .expect("publish");
    }

    for subscription in ["{orders}", "jobs", "events", "refunds.*"] {
        tb.broker::<RedisBroker>()
            .subscriber(subscription)
            .assert_called_once()
            .settled(HandlerOutcome::ack());
    }
    tb.broker::<RedisBroker>()
        .published::<Order>("audit")
        .assert_called(4);
    tb.shutdown().await.expect("shutdown");
}
