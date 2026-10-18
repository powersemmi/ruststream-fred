//! A cluster cannot run a `MULTI` / `EXEC` block across hash slots, so a batch reply committed as
//! one transaction has no cluster to run on: the service is refused at startup, where the mount
//! that asks for it is named, instead of retrying every batch it reads. The live case needs
//! `REDIS_CLUSTER_TEST_URL`.

#![cfg(feature = "testing")]

use ruststream::testing::TestApp;
use ruststream_fred::stream::prelude::*;
use serde::{Deserialize, Serialize};

mod live;

/// The addresses the service's broker is built with; the in-process mode dials nothing.
const STANDALONE: &str = "redis://localhost:6379";
const CLUSTER: &str = "localhost:7000";

#[derive(Debug, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

#[subscriber(RedisStream::new("orders").group("workers"), reply("processed"))]
async fn process(orders: &[Order]) -> Result<Vec<Order>, HandlerOutcome> {
    Ok(orders.iter().map(|order| Order { id: order.id }).collect())
}

/// The service's app on the given topology: one batch handler whose replies commit together.
fn app(broker: RedisBroker) -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(process.batch(nonzero!(2)))
            .out_reply(TransactionalPublish)
            .transactional();
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_transactional_batch_reply_is_refused_on_a_cluster_at_startup() {
    let err = TestApp::start(app(RedisBroker::cluster([CLUSTER])))
        .await
        .expect_err("a cluster cannot commit a batch's replies as one block");
    assert!(
        format!("{err}").contains("only supported on standalone and sentinel"),
        "got {err}"
    );
}

/// A real cluster refuses the same mount the same way, before any batch is read.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_live_cluster_refuses_a_transactional_batch_reply_at_startup() {
    let Some(node) = live::url("REDIS_CLUSTER_TEST_URL") else {
        return;
    };
    let err = TestApp::start_live(app(RedisBroker::cluster([node])))
        .await
        .expect_err("a cluster cannot commit a batch's replies as one block");
    assert!(
        format!("{err}").contains("only supported on standalone and sentinel"),
        "got {err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_transactional_batch_reply_commits_on_standalone() {
    let tb = TestApp::start(app(RedisBroker::standalone(STANDALONE)))
        .await
        .expect("start");

    for id in 0..2 {
        tb.broker::<RedisBroker>()
            .message(&Order { id })
            .to("orders")
            .publish()
            .await
            .expect("publish");
    }

    tb.broker::<RedisBroker>()
        .published::<Order>("processed")
        .assert_called(2);

    tb.shutdown().await.expect("shutdown");
}
