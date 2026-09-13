//! The claiming read mode, driven through the application harness.
//!
//! `RedisStream::claiming` reads new entries and re-reads the group's stale pending ones in a
//! single `XREADGROUP ... CLAIM`, and every delivery carries the server's own idle time and
//! delivery count. Those two counters are the subject here: a fresh entry reports neither, an
//! entry the handler did not finish comes back once it has been idle long enough with its count
//! raised, and a handler that watches the count stops retrying on its own.
//!
//! The stand-in answers the mode the way the server does, so these cases are the in-process twins
//! of the live ones in `tests/integration_fred.rs`.

#![cfg(feature = "testing")]

use std::time::Duration;

use ruststream::testing::TestApp;
use ruststream_fred::stream::prelude::*;
use ruststream_fred::testing::RedisTestBroker;
use ruststream_fred::{DELIVERY_COUNT_HEADER, IDLE_MS_HEADER};
use serde::{Deserialize, Serialize};

/// How long an entry has to sit in the pending entries list before the subscription claims it
/// back. Well above any handler runtime, as the mode requires.
const MIN_IDLE: Duration = Duration::from_secs(30);

/// The cap the capped handler gives up at: the third delivery is the last one it asks for.
const MAX_DELIVERIES: u64 = 2;

#[derive(Debug, Deserialize, Outgoing, PartialEq, Serialize)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

/// The same message on the stream the capped handler reads.
#[derive(Debug, Deserialize, Outgoing, PartialEq, Serialize)]
#[outgoing(name = "flaky")]
struct FlakyOrder {
    id: u64,
}

/// What a delivery reported about itself, published so the test can read it back.
#[derive(Debug, Deserialize, Outgoing, PartialEq, Serialize)]
#[outgoing(name = "orders.seen")]
struct Seen {
    id: u64,
    delivery_count: u64,
    idle_ms: u64,
}

#[derive(OutSlot)]
#[publishes(Seen)]
struct Audit;

/// Retries its first delivery and acknowledges the claim that comes back, reporting the two
/// counters it saw each time.
#[subscriber(RedisStream::claiming("orders", MIN_IDLE).group("workers"))]
async fn handle_order(
    order: &Order,
    ctx: &mut Context<'_>,
    Out(audit): Out<impl Publisher, Audit>,
) -> HandlerOutcome {
    let seen = Seen {
        id: order.id,
        delivery_count: counter(ctx, DELIVERY_COUNT_HEADER),
        idle_ms: counter(ctx, IDLE_MS_HEADER),
    };
    let first = seen.delivery_count == 0;
    if audit.message(&seen).publish().await.is_err() {
        return HandlerOutcome::drop();
    }
    if first {
        HandlerOutcome::retry()
    } else {
        HandlerOutcome::ack()
    }
}

/// Gives up once the server says the entry has been delivered `MAX_DELIVERIES` times: the count a
/// claim raises is the retry counter, so the cap is an ordinary `if` in the body.
#[subscriber(RedisStream::claiming("flaky", MIN_IDLE).group("workers"))]
async fn handle_flaky(
    order: &FlakyOrder,
    ctx: &mut Context<'_>,
    Out(audit): Out<impl Publisher, Audit>,
) -> HandlerOutcome {
    let seen = Seen {
        id: order.id,
        delivery_count: counter(ctx, DELIVERY_COUNT_HEADER),
        idle_ms: counter(ctx, IDLE_MS_HEADER),
    };
    let give_up = seen.delivery_count >= MAX_DELIVERIES;
    if audit.message(&seen).publish().await.is_err() {
        return HandlerOutcome::drop();
    }
    if give_up {
        HandlerOutcome::drop()
    } else {
        HandlerOutcome::retry()
    }
}

/// One counter of a claiming delivery. Reading it is also the assertion that it is there: every
/// delivery of this mode carries both, so a missing one is the defect the test is looking for.
fn counter<Kind, AppState>(ctx: &Context<'_, Kind, AppState>, name: &str) -> u64 {
    ctx.headers()
        .get_str(name)
        .unwrap_or_else(|| panic!("a claiming delivery must carry the {name} header"))
        .parse()
        .unwrap_or_else(|err| panic!("the {name} header must be a number: {err}"))
}

/// The service both cases run against: two claiming subscriptions, each reporting what it saw.
async fn start() -> TestApp<()> {
    let app =
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(RedisTestBroker::new(), |b| {
            b.include(handle_order).out(Audit, Publish).build();
            b.include(handle_flaky).out(Audit, Publish).build();
        });
    TestApp::start(app).await.expect("start")
}

/// A fresh entry has never been claimed, so the server reports no idle time and no earlier
/// delivery. A handler written against the reclaim path reads the same two headers here.
#[tokio::test(start_paused = true)]
async fn a_fresh_entry_reports_no_idle_time_and_no_earlier_delivery() {
    let tb = start().await;

    tb.broker::<RedisTestBroker>()
        .message(&Order { id: 7 })
        .publish()
        .await
        .expect("publish");

    tb.broker::<RedisTestBroker>()
        .subscriber("orders")
        .assert_called_once();
    tb.broker::<RedisTestBroker>()
        .published::<Seen>("orders.seen")
        .with(&Seen {
            id: 7,
            delivery_count: 0,
            idle_ms: 0,
        });

    tb.shutdown().await.expect("shutdown");
}

/// A retry on this mode leaves the entry in the pending entries list instead of appending a copy,
/// so the entry comes back through the subscription's own next read: not before it has been idle
/// `min_idle`, and then with the delivery count one higher.
#[tokio::test(start_paused = true)]
async fn an_entry_left_pending_comes_back_with_its_delivery_count_raised() {
    let tb = start().await;

    tb.broker::<RedisTestBroker>()
        .message(&Order { id: 7 })
        .publish()
        .await
        .expect("publish");
    tb.broker::<RedisTestBroker>()
        .subscriber("orders")
        .assert_called_once()
        .settled(HandlerOutcome::retry());

    // Half the threshold is not the threshold: the entry is still someone's to finish.
    tb.advance(MIN_IDLE / 2).await.expect("advance");
    tb.broker::<RedisTestBroker>()
        .subscriber("orders")
        .assert_called(1);

    tb.advance(MIN_IDLE).await.expect("advance");
    tb.broker::<RedisTestBroker>()
        .subscriber("orders")
        .assert_called(2);
    tb.broker::<RedisTestBroker>()
        .published::<Seen>("orders.seen")
        .with(&Seen {
            id: 7,
            delivery_count: 1,
            idle_ms: 30_000,
        });

    tb.shutdown().await.expect("shutdown");
}

/// The delivery count is the retry counter of this mode, so a handler caps its own retries by
/// reading it. Once the handler gives up, nothing comes back however long the clock runs.
#[tokio::test(start_paused = true)]
async fn a_handler_stops_retrying_at_the_delivery_count_it_caps_on() {
    let tb = start().await;

    tb.broker::<RedisTestBroker>()
        .message(&FlakyOrder { id: 3 })
        .publish()
        .await
        .expect("publish");
    for _ in 0..3 {
        tb.advance(MIN_IDLE).await.expect("advance");
    }

    tb.broker::<RedisTestBroker>()
        .subscriber("flaky")
        .assert_called(3);
    assert_eq!(
        tb.broker::<RedisTestBroker>()
            .published::<Seen>("orders.seen")
            .decoded()
            .iter()
            .map(|seen| seen.delivery_count)
            .collect::<Vec<_>>(),
        [0, 1, 2],
        "each claim raises the count the handler caps on",
    );

    tb.shutdown().await.expect("shutdown");
}
