//! The production routes spelling, run in process.
//!
//! One module per transport form, each globbing that form's own prelude and writing the mount the
//! way a service writes it: `include(handler).out_reply(Publish)`, with the descriptor and the
//! policy named by the words the prelude gives them. The app is the one the service ships, on
//! `RedisBroker`, and the harness runs it in process.
//!
//! The reply is asserted through the broker's publish log, which decodes a list or Pub/Sub reply
//! from the envelope it was framed in.

#![cfg(feature = "testing")]

/// The address the service's broker is built with; the in-process mode dials nothing.
const URL: &str = "redis://localhost:6379";

mod stream_routes {
    use super::URL;
    use ruststream::testing::TestApp;
    use ruststream_fred::stream::prelude::*;
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

    /// The service's app: what `main` runs, and what the case hands the harness.
    fn app() -> impl App<State = ()> {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
            RedisBroker::standalone(URL),
            |b| {
                b.include(confirm).out_reply(Publish);
            },
        )
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_stream_mount_replies_through_its_own_policy() {
        let tb = TestApp::start(app()).await.expect("start");

        tb.broker::<RedisBroker>()
            .publish("orders", &Order { id: 7 })
            .await
            .expect("publish");

        tb.broker::<RedisBroker>()
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
    use super::URL;
    use ruststream::testing::TestApp;
    use ruststream_fred::list::prelude::*;
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

    /// The service's app: what `main` runs, and what the case hands the harness.
    fn app() -> impl App<State = ()> {
        RustStream::new(AppInfo::new("jobs", "0.1.0")).with_broker(
            RedisBroker::standalone(URL),
            |b| {
                b.include(run_job).out_reply(Publish::default());
            },
        )
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_list_mount_replies_through_its_own_policy() {
        let tb = TestApp::start(app()).await.expect("start");

        tb.broker::<RedisBroker>()
            .publish("jobs", &Job { id: 11 })
            .await
            .expect("publish");

        tb.broker::<RedisBroker>()
            .published::<Receipt>("receipts")
            .assert_called_once()
            .with(&Receipt { id: 11 });

        tb.shutdown().await.expect("shutdown");
    }
}

mod pubsub_routes {
    use super::URL;
    use ruststream::testing::TestApp;
    use ruststream_fred::pubsub::prelude::*;
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

    /// The service's app: what `main` runs, and what the case hands the harness.
    fn app() -> impl App<State = ()> {
        RustStream::new(AppInfo::new("events", "0.1.0")).with_broker(
            RedisBroker::standalone(URL),
            |b| {
                b.include(on_event).out_reply(Publish::default());
            },
        )
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_pubsub_mount_replies_through_its_own_policy() {
        let tb = TestApp::start(app()).await.expect("start");

        tb.broker::<RedisBroker>()
            .publish("events", &Event { id: 3 })
            .await
            .expect("publish");

        tb.broker::<RedisBroker>()
            .published::<Audit>("audit")
            .assert_called_once()
            .with(&Audit { id: 3 });

        tb.shutdown().await.expect("shutdown");
    }
}

/// A handler that replies without naming a policy at the mount site replies through the broker's
/// default policy.
mod default_reply {
    use super::URL;
    use ruststream::testing::TestApp;
    use ruststream_fred::stream::prelude::*;
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

    /// The service's app: what `main` runs, and what the case hands the harness.
    fn app() -> impl App<State = ()> {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
            RedisBroker::standalone(URL),
            |b| {
                b.include(confirm);
            },
        )
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_unnamed_reply_policy_still_publishes() {
        let tb = TestApp::start(app()).await.expect("start");

        tb.broker::<RedisBroker>()
            .publish("orders", &Order { id: 2 })
            .await
            .expect("publish");

        tb.broker::<RedisBroker>()
            .published::<Confirmation>("confirmations")
            .assert_called_once()
            .with(&Confirmation { id: 2 });

        tb.shutdown().await.expect("shutdown");
    }
}
