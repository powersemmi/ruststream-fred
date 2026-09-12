//! The production routes spelling, mounted on the in-process stand-in.
//!
//! One module per transport form, each globbing that form's own prelude and writing the mount the
//! way a service writes it: `include(handler).out(Reply, Publish)`, with the descriptor and the
//! policy named by the words the prelude gives them. Nothing here names a test-only type, which is
//! the contract these cases hold: the same wiring pairs against `RedisBroker` and against
//! `RedisTestBroker`, so a service is tested on what it ships.
//!
//! The reply is asserted through the broker's publish log. On a real server a list or Pub/Sub reply
//! is framed with its envelope codec, which the stand-in does not apply, so a decoded assertion
//! like these reads the bare payload here.

#![cfg(feature = "testing")]

mod stream_routes {
    use ruststream::testing::TestApp;
    use ruststream_fred::stream::prelude::*;
    use ruststream_fred::testing::RedisTestBroker;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Deserialize, Serialize)]
    struct Order {
        id: u64,
    }

    #[derive(Debug, Deserialize, Outgoing, Serialize, PartialEq)]
    struct Confirmation {
        id: u64,
        accepted: bool,
    }

    #[subscriber(RedisStream::new("orders").group("workers"), publish("confirmations"))]
    async fn confirm(order: &Order) -> Confirmation {
        Confirmation {
            id: order.id,
            accepted: true,
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_stream_mount_replies_through_its_own_policy() {
        let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
            RedisTestBroker::new(),
            |b| {
                b.include(confirm).out(Reply, Publish);
            },
        );
        let tb = TestApp::start(app).await.expect("start");

        tb.broker::<RedisTestBroker>()
            .publish("orders", &Order { id: 7 })
            .await
            .expect("publish");

        tb.broker::<RedisTestBroker>()
            .published::<Confirmation>("confirmations")
            .assert_called_once()
            .with(&Confirmation {
                id: 7,
                accepted: true,
            });

        tb.shutdown().await.expect("shutdown");
    }
}

mod list_routes {
    use ruststream::testing::TestApp;
    use ruststream_fred::list::prelude::*;
    use ruststream_fred::testing::RedisTestBroker;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Deserialize, Serialize)]
    struct Job {
        id: u64,
    }

    #[derive(Debug, Deserialize, Outgoing, Serialize, PartialEq)]
    struct Receipt {
        id: u64,
    }

    #[subscriber(RedisList::new("jobs").reliable(), publish("receipts"))]
    async fn run_job(job: &Job) -> Receipt {
        Receipt { id: job.id }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_list_mount_replies_through_its_own_policy() {
        let app = RustStream::new(AppInfo::new("jobs", "0.1.0")).with_broker(
            RedisTestBroker::new(),
            |b| {
                b.include(run_job).out(Reply, Publish::default());
            },
        );
        let tb = TestApp::start(app).await.expect("start");

        tb.broker::<RedisTestBroker>()
            .publish("jobs", &Job { id: 11 })
            .await
            .expect("publish");

        tb.broker::<RedisTestBroker>()
            .published::<Receipt>("receipts")
            .assert_called_once()
            .with(&Receipt { id: 11 });

        tb.shutdown().await.expect("shutdown");
    }
}

mod pubsub_routes {
    use ruststream::testing::TestApp;
    use ruststream_fred::pubsub::prelude::*;
    use ruststream_fred::testing::RedisTestBroker;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Deserialize, Serialize)]
    struct Event {
        id: u64,
    }

    #[derive(Debug, Deserialize, Outgoing, Serialize, PartialEq)]
    struct Audit {
        id: u64,
    }

    #[subscriber(RedisPubSub::new("events"), publish("audit"))]
    async fn on_event(event: &Event) -> Audit {
        Audit { id: event.id }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_pubsub_mount_replies_through_its_own_policy() {
        let app = RustStream::new(AppInfo::new("events", "0.1.0")).with_broker(
            RedisTestBroker::new(),
            |b| {
                b.include(on_event).out(Reply, Publish::default());
            },
        );
        let tb = TestApp::start(app).await.expect("start");

        tb.broker::<RedisTestBroker>()
            .publish("events", &Event { id: 3 })
            .await
            .expect("publish");

        tb.broker::<RedisTestBroker>()
            .published::<Audit>("audit")
            .assert_called_once()
            .with(&Audit { id: 3 });

        tb.shutdown().await.expect("shutdown");
    }
}

/// The default reply publisher of the stand-in is the production stream policy, so a handler that
/// replies without naming a policy at the mount site works here exactly as it does in production.
mod default_reply {
    use ruststream::testing::TestApp;
    use ruststream_fred::stream::prelude::*;
    use ruststream_fred::testing::RedisTestBroker;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Deserialize, Serialize)]
    struct Order {
        id: u64,
    }

    #[derive(Debug, Deserialize, Outgoing, Serialize, PartialEq)]
    struct Confirmation {
        id: u64,
    }

    #[subscriber(RedisStream::new("orders").group("workers"), publish("confirmations"))]
    async fn confirm(order: &Order) -> Confirmation {
        Confirmation { id: order.id }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_unnamed_reply_policy_still_publishes() {
        let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
            RedisTestBroker::new(),
            |b| {
                b.include(confirm);
            },
        );
        let tb = TestApp::start(app).await.expect("start");

        tb.broker::<RedisTestBroker>()
            .publish("orders", &Order { id: 2 })
            .await
            .expect("publish");

        tb.broker::<RedisTestBroker>()
            .published::<Confirmation>("confirmations")
            .assert_called_once()
            .with(&Confirmation { id: 2 });

        tb.shutdown().await.expect("shutdown");
    }
}
