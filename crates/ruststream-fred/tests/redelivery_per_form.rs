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
use ruststream_fred::{AtomicList, AtomicStream, PipelinedPubSub, PipelinedStream};
use serde::{Deserialize, Serialize};

/// The address the service's broker is built with; the in-process mode dials nothing.
const URL: &str = "redis://localhost:6379";

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
                RedisBroker::standalone(URL),
                |b| {
                    b.include($handler);
                },
            );
            let tb = TestApp::start(app).await.expect("start");

            tb.broker::<RedisBroker>()
                .message(&Order { id: 1 })
                .to($name)
                .publish()
                .await
                .expect("publish");
            tb.broker::<RedisBroker>()
                .subscriber($name)
                .assert_called_once()
                .settled(HandlerOutcome::retry_after(DELAY));

            // Half the delay is not the delay.
            tb.advance(DELAY / 2).await.expect("advance");
            tb.broker::<RedisBroker>()
                .subscriber($name)
                .assert_called_once();

            tb.advance(DELAY).await.expect("advance");
            tb.broker::<RedisBroker>()
                .subscriber($name)
                .assert_called(2)
                .with(&Order { id: 1 });
            tb.broker::<RedisBroker>()
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
                RedisBroker::standalone(URL),
                |b| {
                    b.include($handler)
                        .max_attempts(nonzero!(2u32))
                        .dead_letter($dead);
                    b.include($reader);
                },
            );
            let tb = TestApp::start(app).await.expect("start");

            tb.broker::<RedisBroker>()
                .message(&Order { id: 2 })
                .to($name)
                .publish()
                .await
                .expect("publish");
            tb.advance(DELAY).await.expect("advance");

            tb.broker::<RedisBroker>()
                .subscriber($name)
                .assert_called(2);
            tb.broker::<RedisBroker>()
                .published::<Order>($dead)
                .assert_called_once()
                .with(&Order { id: 2 })
                .with_header(RETRY_COUNT_HEADER, "2");
            tb.broker::<RedisBroker>()
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

#[subscriber(RedisList::new("jobs").reliable())]
async fn job_to_orders(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::retry()
}

#[subscriber(RedisStream::new("orders").group("workers"))]
async fn order_reader(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::ack()
}

/// A list's dead-letter move is an `LPUSH`, and a key a stream subscription reads cannot take one,
/// so the service refuses to start and names both.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dead_letter_read_as_another_type_refuses_to_start() {
    let app = RustStream::new(AppInfo::new("redelivery", "0.1.0")).with_broker(
        RedisBroker::standalone(URL),
        |b| {
            b.include(order_reader);
            b.include(job_to_orders)
                .max_attempts(nonzero!(2u32))
                .dead_letter("orders");
        },
    );
    let err = TestApp::start(app)
        .await
        .expect_err("a name is one Redis type");
    let text = format!("{err:?}");
    assert!(
        text.contains("orders") && text.contains("list") && text.contains("stream"),
        "the refusal names the name and both types: {text}"
    );
}

#[subscriber(RedisStream::new("orders").group("workers"), publish("jobs"))]
async fn order_to_job(order: &Order) -> Order {
    Order { id: order.id }
}

#[subscriber(RedisList::new("jobs").reliable())]
async fn job_reader(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::ack()
}

/// A reply the mount site names no publisher for leaves through the default one, which writes a
/// name this service reads as a list with `LPUSH`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_default_reply_reaches_a_list_this_service_reads() {
    let app = RustStream::new(AppInfo::new("redelivery", "0.1.0")).with_broker(
        RedisBroker::standalone(URL),
        |b| {
            b.include(order_to_job);
            b.include(job_reader);
        },
    );
    let tb = TestApp::start(app).await.expect("start");

    tb.broker::<RedisBroker>()
        .message(&Order { id: 3 })
        .to("orders")
        .publish()
        .await
        .expect("publish");

    tb.broker::<RedisBroker>()
        .subscriber("jobs")
        .assert_called_once()
        .with(&Order { id: 3 });

    tb.shutdown().await.expect("shutdown");
}

// The same with a window: a retry's settle rides the window, and the copy and the move leave
// through the form's own publish.

#[subscriber(PipelinedStream::new("orders").group("workers"))]
async fn windowed_stream_once(order: &Order, ctx: &mut Context<'_>) -> HandlerOutcome {
    let _ = order;
    once(ctx.headers().get(RETRY_COUNT_HEADER).is_some())
}

#[subscriber(AtomicList::new("jobs").reliable())]
async fn atomic_list_once(order: &Order, ctx: &mut Context<'_>) -> HandlerOutcome {
    let _ = order;
    once(ctx.headers().get(RETRY_COUNT_HEADER).is_some())
}

#[subscriber(PipelinedPubSub::new("events"))]
async fn windowed_channel_once(order: &Order, ctx: &mut Context<'_>) -> HandlerOutcome {
    let _ = order;
    once(ctx.headers().get(RETRY_COUNT_HEADER).is_some())
}

comes_back_once!(
    a_windowed_stream_delivery_comes_back_once,
    windowed_stream_once,
    "orders"
);
comes_back_once!(
    an_atomic_list_delivery_comes_back_once,
    atomic_list_once,
    "jobs"
);
comes_back_once!(
    a_windowed_channel_delivery_comes_back_once,
    windowed_channel_once,
    "events"
);

#[subscriber(AtomicStream::new("orders").group("workers"))]
async fn atomic_stream_never(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::retry_after(DELAY)
}

#[subscriber(AtomicList::new("jobs").reliable())]
async fn atomic_list_never(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::retry_after(DELAY)
}

#[subscriber(PipelinedPubSub::new("events"))]
async fn windowed_channel_never(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::retry_after(DELAY)
}

moves_at_the_cap!(
    an_atomic_stream_moves_at_the_cap,
    atomic_stream_never,
    "orders",
    stream_dead,
    "orders.dead"
);
moves_at_the_cap!(
    an_atomic_list_moves_at_the_cap,
    atomic_list_never,
    "jobs",
    reliable_dead,
    "jobs.dead"
);
moves_at_the_cap!(
    a_windowed_channel_moves_at_the_cap,
    windowed_channel_never,
    "events",
    channel_dead,
    "events.dead"
);
