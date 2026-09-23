//! The runtime's redelivery on every subscription form, with nothing named at the mount site.
//!
//! A delivery that asks for `retry_after` comes back once, after the delay, with the framework's
//! retry count one higher; at the cap it moves to the dead-letter destination. The copy and the
//! move leave through the publish of the form the delivery came from: `XADD` for a stream,
//! `LPUSH` for a list, `PUBLISH` for a channel. The in-process broker refuses a publish of another
//! form there, as a server would, so a registration whose copies left the wrong way fails here.

#![cfg(feature = "testing")]

use std::time::Duration;

use ruststream::runtime::RETRY_COUNT_HEADER;
use ruststream::testing::TestApp;
use ruststream_fred::prelude::*;
use ruststream_fred::testing::RedisTestBroker;
use serde::{Deserialize, Serialize};

const DELAY: Duration = Duration::from_secs(30);

#[derive(Debug, Deserialize, Outgoing, PartialEq, Serialize)]
struct Order {
    id: u64,
}

/// Asks for the delay on the first delivery and acknowledges the copy.
fn once(copy: bool) -> HandlerOutcome {
    if copy {
        HandlerOutcome::ack()
    } else {
        HandlerOutcome::retry_after(DELAY)
    }
}

#[subscriber(RedisStream::new("orders").group("workers"))]
async fn stream_once(order: &Order, ctx: &mut Context<'_>) -> HandlerOutcome {
    let _ = order;
    once(ctx.headers().get(RETRY_COUNT_HEADER).is_some())
}

#[subscriber(RedisList::new("jobs").reliable())]
async fn reliable_once(order: &Order, ctx: &mut Context<'_>) -> HandlerOutcome {
    let _ = order;
    once(ctx.headers().get(RETRY_COUNT_HEADER).is_some())
}

#[subscriber(RedisList::new("tasks"))]
async fn plain_once(order: &Order, ctx: &mut Context<'_>) -> HandlerOutcome {
    let _ = order;
    once(ctx.headers().get(RETRY_COUNT_HEADER).is_some())
}

#[subscriber(RedisPubSub::new("events"))]
async fn channel_once(order: &Order, ctx: &mut Context<'_>) -> HandlerOutcome {
    let _ = order;
    once(ctx.headers().get(RETRY_COUNT_HEADER).is_some())
}

/// Mounts `handler` alone and checks the round trip of one delayed copy on `name`.
macro_rules! comes_back_once {
    ($test:ident, $handler:ident, $name:literal) => {
        #[tokio::test(start_paused = true)]
        async fn $test() {
            let app = RustStream::new(AppInfo::new("redelivery", "0.1.0")).with_broker(
                RedisTestBroker::new(),
                |b| {
                    b.include($handler);
                },
            );
            let tb = TestApp::start(app).await.expect("start");

            tb.broker::<RedisTestBroker>()
                .message(&Order { id: 1 })
                .to($name)
                .publish()
                .await
                .expect("publish");
            tb.broker::<RedisTestBroker>()
                .subscriber($name)
                .assert_called_once()
                .settled(HandlerOutcome::retry_after(DELAY));

            // Half the delay is not the delay.
            tb.advance(DELAY / 2).await.expect("advance");
            tb.broker::<RedisTestBroker>()
                .subscriber($name)
                .assert_called_once();

            tb.advance(DELAY).await.expect("advance");
            tb.broker::<RedisTestBroker>()
                .subscriber($name)
                .assert_called(2)
                .with(&Order { id: 1 });
            tb.broker::<RedisTestBroker>()
                .published::<Order>($name)
                .assert_called(2)
                .with_header(RETRY_COUNT_HEADER, "1");

            tb.shutdown().await.expect("shutdown");
        }
    };
}

comes_back_once!(a_stream_delivery_comes_back_once, stream_once, "orders");
comes_back_once!(
    a_reliable_list_delivery_comes_back_once,
    reliable_once,
    "jobs"
);
comes_back_once!(a_plain_list_delivery_comes_back_once, plain_once, "tasks");
comes_back_once!(a_channel_delivery_comes_back_once, channel_once, "events");

#[subscriber(RedisStream::new("orders").group("workers"))]
async fn stream_never(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::retry_after(DELAY)
}

#[subscriber(RedisList::new("jobs").reliable())]
async fn reliable_never(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::retry_after(DELAY)
}

#[subscriber(RedisList::new("tasks"))]
async fn plain_never(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::retry_after(DELAY)
}

#[subscriber(RedisPubSub::new("events"))]
async fn channel_never(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::retry_after(DELAY)
}

/// Reads each dead-letter destination the way its form reads, so a move that left through another
/// form's publish is refused there rather than written where nobody reads it.
#[subscriber(RedisStream::new("orders.dead").group("operators"))]
async fn stream_dead(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::ack()
}

#[subscriber(RedisList::new("jobs.dead").reliable())]
async fn reliable_dead(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::ack()
}

#[subscriber(RedisList::new("tasks.dead"))]
async fn plain_dead(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::ack()
}

#[subscriber(RedisPubSub::new("events.dead"))]
async fn channel_dead(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::ack()
}

/// Mounts `handler` under a cap of two, beside the reader of its dead-letter destination, and
/// checks the second delivery moves there.
macro_rules! moves_at_the_cap {
    ($test:ident, $handler:ident, $name:literal, $reader:ident, $dead:literal) => {
        #[tokio::test(start_paused = true)]
        async fn $test() {
            let app = RustStream::new(AppInfo::new("redelivery", "0.1.0")).with_broker(
                RedisTestBroker::new(),
                |b| {
                    b.include($handler)
                        .max_attempts(nonzero!(2u32))
                        .dead_letter($dead);
                    b.include($reader);
                },
            );
            let tb = TestApp::start(app).await.expect("start");

            tb.broker::<RedisTestBroker>()
                .message(&Order { id: 2 })
                .to($name)
                .publish()
                .await
                .expect("publish");
            tb.advance(DELAY).await.expect("advance");

            tb.broker::<RedisTestBroker>()
                .subscriber($name)
                .assert_called(2);
            tb.broker::<RedisTestBroker>()
                .published::<Order>($dead)
                .assert_called_once()
                .with(&Order { id: 2 })
                .with_header(RETRY_COUNT_HEADER, "2");
            tb.broker::<RedisTestBroker>()
                .subscriber($dead)
                .assert_called_once();

            tb.shutdown().await.expect("shutdown");
        }
    };
}

moves_at_the_cap!(
    a_stream_moves_at_the_cap,
    stream_never,
    "orders",
    stream_dead,
    "orders.dead"
);
moves_at_the_cap!(
    a_reliable_list_moves_at_the_cap,
    reliable_never,
    "jobs",
    reliable_dead,
    "jobs.dead"
);
moves_at_the_cap!(
    a_plain_list_moves_at_the_cap,
    plain_never,
    "tasks",
    plain_dead,
    "tasks.dead"
);
moves_at_the_cap!(
    a_channel_moves_at_the_cap,
    channel_never,
    "events",
    channel_dead,
    "events.dead"
);
