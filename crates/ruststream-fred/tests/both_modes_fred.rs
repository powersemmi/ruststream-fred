//! One test body, run twice: in process with `TestApp::start`, and against the live stand with
//! `TestApp::start_live`. Only the start call differs.
//!
//! The body covers the three forms of Redis delivery the service uses: a stream group that
//! replies, a stream whose delay queue serves a retry, and a Pub/Sub pattern subscription that
//! hears what another handler announces. The live run needs `REDIS_TEST_URL`; under
//! `RUSTSTREAM_REQUIRE_LIVE` a missing address fails it instead of skipping.

#![cfg(feature = "testing")]

use std::error::Error;
use std::time::Duration;

use ruststream::runtime::RETRY_COUNT_HEADER;
use ruststream::testing::TestApp;
use ruststream_fred::prelude::*;
use serde::{Deserialize, Serialize};

mod live;

/// The address the in-process run builds the broker with; nothing dials it.
const IN_PROCESS_URL: &str = "redis://localhost:6379";

/// How long the invoice handler asks its retry to wait.
const DELAY: Duration = Duration::from_millis(500);

/// A short read block, so a live subscription wakes promptly for its delay queue.
const BLOCK: Duration = Duration::from_millis(50);

#[derive(Debug, Deserialize, Outgoing, PartialEq, Serialize)]
struct Order {
    id: u64,
}

#[derive(Debug, Deserialize, Outgoing, PartialEq, Serialize)]
#[outgoing(name = "both.confirmations")]
struct Confirmation {
    id: u64,
}

#[derive(Debug, Deserialize, Outgoing, PartialEq, Serialize)]
#[outgoing(name = "both.events.eu")]
struct Announced {
    id: u64,
}

#[subscriber(RedisStream::new("both.orders").group("workers").block(BLOCK), publish)]
async fn confirm(order: &Order) -> Confirmation {
    Confirmation { id: order.id }
}

/// Asks for one delayed retry, which the stream's delay queue serves, and accepts the copy it adds
/// back: that one carries the retry count the queue raised.
#[subscriber(
    RedisStream::new("both.invoices")
        .group("workers")
        .block(BLOCK)
        .delayed_retry(DelayedRetry::DurableZset { key: "both.invoices.delayed".to_owned(), ttl: None })
)]
async fn invoice(order: &Order, ctx: &mut Context<'_>) -> HandlerOutcome {
    let _ = order.id;
    if ctx.headers().get_str(RETRY_COUNT_HEADER).is_some() {
        HandlerOutcome::ack()
    } else {
        HandlerOutcome::retry_after(DELAY)
    }
}

#[subscriber(RedisStream::new("both.announcements").group("workers").block(BLOCK), publish)]
async fn announce(order: &Order) -> Announced {
    Announced { id: order.id }
}

#[subscriber(RedisPubSubPattern::new("both.events.*"))]
async fn hear(event: &Announced) -> HandlerOutcome {
    let _ = event.id;
    HandlerOutcome::ack()
}

/// The service's app, on the broker `main` builds from its address.
fn app(url: &str) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("both", "0.1.0")).with_broker(RedisBroker::standalone(url), |b| {
        b.include(confirm);
        b.include(invoice);
        b.include(announce).out_reply(pubsub::Publish::new());
        b.include(hear)
            .out_retry(pubsub::Publish::new())
            .to("both.events.retry");
    })
}

/// The body both modes run.
async fn a_service_confirms_retries_and_hears_its_announcements(
    tb: TestApp<()>,
) -> Result<(), Box<dyn Error>> {
    let redis = tb.broker::<RedisBroker>();

    redis
        .message(&Order { id: 1 })
        .to("both.orders")
        .publish()
        .await?;
    redis
        .subscriber("both.orders")
        .assert_called_once()
        .with(&Order { id: 1 })
        .settled(HandlerOutcome::ack());
    redis
        .published::<Confirmation>("both.confirmations")
        .assert_called_once()
        .with(&Confirmation { id: 1 });

    redis
        .message(&Order { id: 2 })
        .to("both.invoices")
        .publish()
        .await?;
    redis
        .subscriber("both.invoices")
        .assert_called_once()
        .settled(HandlerOutcome::retry_after(DELAY));
    tb.advance(DELAY).await?;
    redis
        .subscriber("both.invoices")
        .assert_called(2)
        .with(&Order { id: 2 })
        .settled(HandlerOutcome::ack());

    redis
        .message(&Order { id: 3 })
        .to("both.announcements")
        .publish()
        .await?;
    redis
        .subscriber("both.events.*")
        .assert_called_once()
        .with(&Announced { id: 3 });

    tb.shutdown().await?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn in_process() -> Result<(), Box<dyn Error>> {
    a_service_confirms_retries_and_hears_its_announcements(
        TestApp::start(app(IN_PROCESS_URL)).await?,
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live() -> Result<(), Box<dyn Error>> {
    let Some(url) = live::url("REDIS_TEST_URL") else {
        return Ok(());
    };
    a_service_confirms_retries_and_hears_its_announcements(TestApp::start_live(app(&url)).await?)
        .await
}
