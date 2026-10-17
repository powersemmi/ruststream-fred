//! A service keeps working after the server drops its connections.
//!
//! A Redis restart, a sentinel failover and an idle-timeout on a proxy all look the same from the
//! client: every socket it holds is closed under it. The test drops them the way the server does,
//! with `CLIENT KILL` from a connection of its own, so it needs no access to the server's process
//! or container. Before the kill and after it, each form of delivery the service uses (a stream
//! group, a list and a Pub/Sub channel) has to receive a message. A second case deletes the keys
//! with the kill, as a restart of a server that keeps no data does, consumer groups included.
//!
//! `CLIENT KILL TYPE normal` closes every ordinary connection to the server, not only this test's:
//! the live suites run one test at a time (`--test-threads=1`), so nothing else is connected.
//! The run needs `REDIS_TEST_URL`; under `RUSTSTREAM_REQUIRE_LIVE` a missing address fails it.

#![cfg(feature = "testing")]

use std::collections::HashMap;
use std::error::Error;
use std::time::Duration;

use fred::interfaces::{ClientInterface, ClientLike, KeysInterface, PubsubInterface};
use fred::prelude::{Builder, Config};
use fred::types::client::{ClientKillFilter, ClientKillType};
use ruststream::testing::TestApp;
use ruststream_fred::prelude::*;
use serde::{Deserialize, Serialize};

mod live;

const EVENTS: &str = "reconnect.events";

/// The longest the Pub/Sub subscription may take to be back on the server after the kill.
const RESUBSCRIBED_WITHIN: Duration = Duration::from_secs(10);

#[derive(Debug, Deserialize, Outgoing, PartialEq, Serialize)]
struct Order {
    id: u64,
}

#[subscriber(RedisStream::new("reconnect.orders").group("workers"))]
async fn take_order(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

#[subscriber(RedisList::new("reconnect.jobs").reliable())]
async fn run_job(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

#[subscriber(RedisPubSub::new("reconnect.events"))]
async fn hear(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

fn app(url: &str) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("reconnect", "0.1.0")).with_broker(
        RedisBroker::standalone(url),
        |b| {
            b.include(take_order);
            b.include(run_job);
            b.include(hear);
        },
    )
}

/// Publishes `order` to each of the three subscriptions and checks each has now run `times`.
async fn deliver_to_each(
    tb: &TestApp<()>,
    order: &Order,
    times: usize,
) -> Result<(), Box<dyn Error>> {
    let redis = tb.broker::<RedisBroker>();
    redis
        .message(order)
        .to("reconnect.orders")
        .publish()
        .await?;
    redis.message(order).to("reconnect.jobs").publish().await?;
    redis.message(order).to(EVENTS).publish().await?;
    for subscription in ["reconnect.orders", "reconnect.jobs", EVENTS] {
        redis
            .subscriber(subscription)
            .assert_called(times)
            .with(order)
            .settled(HandlerOutcome::ack());
    }
    Ok(())
}

/// How many connections the server has subscribed to `channel`.
async fn subscribers(admin: &fred::clients::Client, channel: &str) -> Result<i64, Box<dyn Error>> {
    let counts: HashMap<String, i64> = admin.pubsub_numsub(channel).await?;
    Ok(counts.get(channel).copied().unwrap_or_default())
}

/// The keys the service reads, which a restart of a server that keeps no data loses.
const KEYS: [&str; 3] = [
    "reconnect.orders",
    "reconnect.jobs",
    "reconnect.jobs.processing",
];

/// What the server does to the service between the two rounds of deliveries.
#[derive(Clone, Copy)]
enum Restart {
    /// Every connection is dropped; the data stays, as on a server that persists it.
    KeepingData,
    /// The data is gone as well, consumer groups included, as on a server that keeps none.
    LosingData,
}

/// Delivers to each subscription, has the server `restart`, and delivers to each again.
async fn deliveries_survive(restart: Restart) -> Result<(), Box<dyn Error>> {
    let Some(url) = live::url("REDIS_TEST_URL") else {
        return Ok(());
    };
    let admin = Builder::from_config(Config::from_url(&url)?).build()?;
    admin.init().await?;
    // A message an interrupted earlier run left behind would be delivered on start.
    let _: i64 = admin.del(KEYS.to_vec()).await?;
    let tb = TestApp::start_live(app(&url)).await?;

    deliver_to_each(&tb, &Order { id: 1 }, 1).await?;

    if matches!(restart, Restart::LosingData) {
        let _: i64 = admin.del(KEYS.to_vec()).await?;
    }
    let filters = |kind| vec![ClientKillFilter::Type(kind), ClientKillFilter::SkipMe(true)];
    let normal: i64 = admin.client_kill(filters(ClientKillType::Normal)).await?;
    let pubsub: i64 = admin.client_kill(filters(ClientKillType::Pubsub)).await?;
    assert!(
        normal > 0,
        "the kill closed none of the service's connections"
    );
    assert_eq!(
        pubsub, 1,
        "the kill closed the Pub/Sub subscription's connection"
    );

    // Why a poll: a message published to a channel nobody is subscribed to is gone, and the
    // service has no signal for "subscribed again" that a test can await. The server's own count
    // is the fact the publish below depends on.
    tokio::time::timeout(RESUBSCRIBED_WITHIN, async {
        while subscribers(&admin, EVENTS).await.unwrap_or_default() < 1 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .map_err(|_| "the Pub/Sub subscription did not come back on the server")?;

    deliver_to_each(&tb, &Order { id: 2 }, 2).await?;

    tb.shutdown().await?;
    admin.quit().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deliveries_resume_after_the_server_drops_every_connection() -> Result<(), Box<dyn Error>> {
    deliveries_survive(Restart::KeepingData).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deliveries_resume_after_the_server_loses_its_data() -> Result<(), Box<dyn Error>> {
    deliveries_survive(Restart::LosingData).await
}
