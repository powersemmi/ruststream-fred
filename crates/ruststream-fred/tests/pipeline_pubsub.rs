//! A `.pipeline()` Pub/Sub subscription, on a channel and on a pattern: Pub/Sub settles nothing,
//! so its window carries the handlers' commands alone, and they leave only for a delivery
//! acknowledged.

#![cfg(feature = "testing")]

use ruststream::testing::TestApp;
use ruststream_fred::context::keys;
use ruststream_fred::prelude::*;
use ruststream_fred::pubsub::Publish;
use serde::{Deserialize, Serialize};

/// The address the service's broker is built with; the in-process mode dials nothing.
const URL: &str = "redis://localhost:6379";

#[derive(Debug, Deserialize, Outgoing, PartialEq, Serialize)]
struct Event {
    id: u64,
    keep: bool,
}

async fn queue_and_settle(
    event: &Event,
    pipeline: &ruststream_fred::pipeline::RedisPipeline,
) -> HandlerOutcome {
    let body = serde_json::to_vec(event).expect("an event encodes");
    if pipeline.lpush("audit", body).await.is_err() {
        return HandlerOutcome::retry();
    }
    if event.keep {
        HandlerOutcome::ack()
    } else {
        HandlerOutcome::drop()
    }
}

#[subscriber(RedisPubSub::new("events"))]
async fn windowed(event: &Event, Ctx(pipeline): Ctx<keys::Pipeline>) -> HandlerOutcome {
    queue_and_settle(event, &pipeline).await
}

#[subscriber(RedisPubSub::new("events"))]
async fn atomic(event: &Event, Ctx(pipeline): Ctx<keys::Pipeline>) -> HandlerOutcome {
    queue_and_settle(event, &pipeline).await
}

#[subscriber(RedisPubSubPattern::new("events.*"))]
async fn windowed_pattern(event: &Event, Ctx(pipeline): Ctx<keys::Pipeline>) -> HandlerOutcome {
    queue_and_settle(event, &pipeline).await
}

#[subscriber(RedisPubSubPattern::new("events.*"))]
async fn atomic_pattern(event: &Event, Ctx(pipeline): Ctx<keys::Pipeline>) -> HandlerOutcome {
    queue_and_settle(event, &pipeline).await
}

/// A pattern subscription, mounted with the channel its retry copies go to: a pattern names none.
macro_rules! pattern_case {
    ($test:ident, $handler:expr, $keep:literal, $expected:literal) => {
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn $test() {
            let app = RustStream::new(AppInfo::new("pipeline", "0.1.0")).with_broker(
                RedisBroker::standalone(URL),
                |b| {
                    b.include($handler)
                        .out_retry(Publish::default())
                        .to("events.retry");
                },
            );
            let tb = TestApp::start(app).await.expect("start");

            tb.broker::<RedisBroker>()
                .message(&Event { id: 1, keep: $keep })
                .to("events.eu")
                .publish()
                .await
                .expect("publish");

            tb.broker::<RedisBroker>()
                .subscriber("events.*")
                .assert_called_once();
            tb.broker::<RedisBroker>()
                .published::<Event>("audit")
                .assert_called($expected);

            tb.shutdown().await.expect("shutdown");
        }
    };
}

pattern_case!(
    an_acknowledged_pattern_event_sends_what_it_queued,
    windowed_pattern.pipeline(),
    true,
    1
);
pattern_case!(
    a_dropped_pattern_event_sends_nothing_it_queued,
    windowed_pattern.pipeline(),
    false,
    0
);
pattern_case!(
    an_acknowledged_atomic_pattern_event_sends_what_it_queued,
    atomic_pattern.pipeline().atomic(),
    true,
    1
);
pattern_case!(
    a_dropped_atomic_pattern_event_sends_nothing_it_queued,
    atomic_pattern.pipeline().atomic(),
    false,
    0
);

macro_rules! case {
    ($test:ident, $handler:expr, $keep:literal, $expected:literal) => {
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn $test() {
            let app = RustStream::new(AppInfo::new("pipeline", "0.1.0")).with_broker(
                RedisBroker::standalone(URL),
                |b| {
                    b.include($handler);
                },
            );
            let tb = TestApp::start(app).await.expect("start");

            tb.broker::<RedisBroker>()
                .message(&Event { id: 1, keep: $keep })
                .to("events")
                .publish()
                .await
                .expect("publish");

            tb.broker::<RedisBroker>()
                .subscriber("events")
                .assert_called_once();
            tb.broker::<RedisBroker>()
                .published::<Event>("audit")
                .assert_called($expected);

            tb.shutdown().await.expect("shutdown");
        }
    };
}

case!(
    an_acknowledged_event_sends_what_it_queued,
    windowed.pipeline(),
    true,
    1
);
case!(
    a_dropped_event_sends_nothing_it_queued,
    windowed.pipeline(),
    false,
    0
);
case!(
    an_acknowledged_atomic_event_sends_what_it_queued,
    atomic.pipeline().atomic(),
    true,
    1
);
case!(
    a_dropped_atomic_event_sends_nothing_it_queued,
    atomic.pipeline().atomic(),
    false,
    0
);
