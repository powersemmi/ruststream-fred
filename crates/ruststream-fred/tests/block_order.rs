//! The `block(..)` step reads in any order around the core's `start_at(..)`, on a subscription
//! the attribute names and on one mounted by hand, with or without the window `.pipeline()` opens.

#![cfg(feature = "testing")]

use std::future::{Future, ready};
use std::time::Duration;

use ruststream::runtime::{Context, Handle, subscriber};
use ruststream::testing::TestApp;
use ruststream_fred::prelude::*;
use serde::{Deserialize, Serialize};

/// The address the service's broker is built with; the in-process mode dials nothing.
const URL: &str = "redis://localhost:6379";

/// How long one read of the subscriptions below blocks.
const BLOCK: Duration = Duration::from_millis(50);

#[derive(Debug, Deserialize, Outgoing, PartialEq, Serialize)]
struct Order {
    id: u64,
}

#[subscriber(
    RedisStream::new("windowed").group("workers"),
    start_at(RedisGroupPosition::beginning())
)]
async fn windowed(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn block_follows_start_at() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(
        RedisBroker::standalone(URL),
        |b| {
            b.include(windowed.pipeline().block(BLOCK));
            b.include(
                subscriber(RedisStream::new("manual").group("workers"), TakeOrder)
                    .undocumented()
                    .build()
                    .start_at(RedisGroupPosition::beginning())
                    .block(BLOCK),
            );
            b.include(
                subscriber(
                    RedisStream::new("manual.windowed").group("workers"),
                    TakeOrder,
                )
                .undocumented()
                .build()
                .start_at(RedisGroupPosition::beginning())
                .pipeline()
                .atomic()
                .block(BLOCK),
            );
        },
    );
    let tb = TestApp::start(app).await.expect("start");

    let keys = ["windowed", "manual", "manual.windowed"];
    for (id, key) in (1..).zip(keys) {
        tb.broker::<RedisBroker>()
            .message(&Order { id })
            .to(key)
            .publish()
            .await
            .expect("publish");
    }

    for (id, key) in (1..).zip(keys) {
        tb.broker::<RedisBroker>()
            .subscriber(key)
            .assert_called_once()
            .with(&Order { id })
            .settled(HandlerOutcome::ack());
    }
    tb.shutdown().await.expect("shutdown");
}
