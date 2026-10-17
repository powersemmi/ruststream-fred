//! A service hands the broker a `fred` pool it built in its synchronous app builder, and the
//! broker connects that pool at startup, as it connects the pool it builds from an address.
//!
//! The in-process mode builds a pool of its own and never dials the adopted one, so these cases
//! run with `TestApp::start_live`. The round trip needs `REDIS_TEST_URL`; under
//! `RUSTSTREAM_REQUIRE_LIVE` a missing address fails it instead of skipping. The refusal dials a
//! port nothing listens on and needs no server.
//!
//! ```bash
//! REDIS_TEST_URL=redis://127.0.0.1:6379 cargo test -p ruststream-fred --all-features \
//!     --test adopted_pool
//! ```

#![cfg(feature = "testing")]

use std::error::Error;
use std::time::Duration;

use fred::clients::Pool;
use fred::types::config::Config;
use ruststream::testing::TestApp;
use ruststream_fred::prelude::*;
use serde::{Deserialize, Serialize};
use tokio::time::timeout;

mod live;

/// An address nothing listens on: the port is reserved and closed.
const UNREACHABLE: &str = "redis://127.0.0.1:1";

/// Longer than a refused dial takes, short enough that a startup which never dials fails the case.
const STARTUP: Duration = Duration::from_secs(10);

#[derive(Debug, Deserialize, Outgoing, PartialEq, Serialize)]
struct Order {
    id: u64,
}

#[derive(Debug, Deserialize, Outgoing, PartialEq, Serialize)]
#[outgoing(name = "adopted.confirmations")]
struct Confirmation {
    id: u64,
}

#[subscriber(
    RedisStream::new("adopted.orders").group("workers").block(Duration::from_millis(50)),
    reply
)]
async fn confirm(order: &Order) -> Confirmation {
    Confirmation { id: order.id }
}

/// The service's app over a pool it built from `url` and did not connect.
fn app(url: &str) -> Result<impl App<State = ()>, Box<dyn Error>> {
    let pool = Pool::new(Config::from_url(url)?, None, None, None, 2)?;
    Ok(
        RustStream::new(AppInfo::new("adopted", "0.1.0")).with_broker(
            RedisBroker::from_pool(pool),
            |b| {
                b.include(confirm);
            },
        ),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pool_built_in_the_app_builder_connects_at_startup() -> Result<(), Box<dyn Error>> {
    let Some(url) = live::url("REDIS_TEST_URL") else {
        return Ok(());
    };
    let tb = timeout(STARTUP, TestApp::start_live(app(&url)?)).await??;
    let redis = tb.broker::<RedisBroker>();

    redis
        .message(&Order { id: 7 })
        .to("adopted.orders")
        .publish()
        .await?;
    redis
        .subscriber("adopted.orders")
        .assert_called_once()
        .with(&Order { id: 7 })
        .settled(HandlerOutcome::ack());
    redis
        .published::<Confirmation>("adopted.confirmations")
        .assert_called_once()
        .with(&Confirmation { id: 7 });

    tb.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_adopted_pool_that_cannot_reach_its_server_fails_startup() -> Result<(), Box<dyn Error>>
{
    let started = timeout(STARTUP, TestApp::start_live(app(UNREACHABLE)?))
        .await
        .map_err(|_| "startup neither connected nor failed")?;

    assert!(started.is_err(), "startup succeeded without a server");
    Ok(())
}
