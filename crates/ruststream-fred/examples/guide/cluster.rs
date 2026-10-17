//! The service of the Redis Cluster guide: a shop backend on a cluster, with a work queue, an
//! order stream whose side effects commit with the acknowledgement, and sharded Pub/Sub events.
//!
//! ```text
//! cargo test --example guide_cluster --features testing
//! just brokers-up
//! REDIS_CLUSTER_TEST_URL=127.0.0.1:7000 cargo test --example guide_cluster --features testing
//! ```

use ruststream_fred::prelude::*;
use serde::{Deserialize, Serialize};

// A hash-tagged key reads as a name template in `#[outgoing(name)]`, so a producer names it.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Job {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "events")]
struct Event {
    kind: String,
}

// --8<-- [start:jobs]
/// The hash tag `{jobs}` keeps the queue and its processing list `{jobs}.processing` on one slot.
#[subscriber(RedisList::new("{jobs}").reliable())]
async fn run_job(job: &Job) -> HandlerOutcome {
    tracing::info!(job.id, "job done");
    HandlerOutcome::ack()
}
// --8<-- [end:jobs]

// --8<-- [start:orders]
/// Invoices an order; the invoice and the acknowledgement commit as one block on the slot of
/// `{orders}`.
#[subscriber(RedisStream::new("{orders}").group("billing"))]
async fn invoice(order: &Order, Ctx(pipeline): Ctx<keys::Pipeline>) -> HandlerOutcome {
    if pipeline
        .lpush("{orders}:invoices", order.id.to_string())
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}
// --8<-- [end:orders]

// --8<-- [start:events]
/// Sharded Pub/Sub: the channel lives on the node that owns its slot.
#[subscriber(RedisPubSub::new("events").mode(PubSubMode::Sharded))]
async fn on_event(event: &Event) -> HandlerOutcome {
    tracing::info!(event.kind, "event received");
    HandlerOutcome::ack()
}
// --8<-- [end:events]

// --8<-- [start:app]
fn service(broker: RedisBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("shop", "0.1.0")).with_broker(broker, |b| {
        b.include(run_job);
        b.include(invoice.pipeline().atomic());
        b.include(on_event);
    })
}

/// Two seeds: the client reaches the cluster through whichever answers first.
#[ruststream::app]
fn app() -> impl App<State = ()> {
    service(RedisBroker::cluster([
        "redis-1.internal:6379",
        "redis-2.internal:6379",
    ]))
}
// --8<-- [end:app]

#[cfg(test)]
#[path = "../../tests/live/mod.rs"]
mod live;

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_fred::prelude::*;

    use super::{Event, Job, Order, app, live, service};

    /// The body both modes run.
    async fn serves_every_form(tb: TestApp<()>) -> Result<(), Box<dyn Error>> {
        let broker = tb.broker::<RedisBroker>();
        broker
            .message(&Job { id: 1 })
            .to("{jobs}")
            .publish()
            .await?;
        broker
            .message(&Order { id: 2 })
            .to("{orders}")
            .publish()
            .await?;
        broker
            .message(&Event {
                kind: "restock".to_owned(),
            })
            .publish()
            .await?;

        broker
            .subscriber("{jobs}")
            .assert_called_once()
            .with(&Job { id: 1 })
            .settled(HandlerOutcome::ack());
        broker
            .subscriber("{orders}")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        broker
            .subscriber("events")
            .assert_called_once()
            .with(&Event {
                kind: "restock".to_owned(),
            });
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn in_process() -> Result<(), Box<dyn Error>> {
        serves_every_form(TestApp::start(app()).await?).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn against_the_stand() -> Result<(), Box<dyn Error>> {
        let Some(node) = live::url("REDIS_CLUSTER_TEST_URL") else {
            return Ok(());
        };
        serves_every_form(TestApp::start_live(service(RedisBroker::cluster([node]))).await?).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_invoice_commits_on_the_slot_of_the_stream() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<RedisBroker>()
            .message(&Order { id: 3 })
            .to("{orders}")
            .publish()
            .await?;

        tb.broker::<RedisBroker>()
            .published::<u64>("{orders}:invoices")
            .assert_called_once()
            .with(&3);
        tb.shutdown().await?;
        Ok(())
    }

    #[subscriber(RedisList::new("jobs").reliable())]
    async fn untagged(job: &Job) -> HandlerOutcome {
        tracing::info!(job.id, "job done");
        HandlerOutcome::ack()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_reliable_list_across_two_slots_does_not_start() {
        let app = RustStream::new(AppInfo::new("shop", "0.1.0")).with_broker(
            RedisBroker::cluster(["redis-1.internal:6379"]),
            |b| {
                b.include(untagged);
            },
        );

        let err = TestApp::start(app)
            .await
            .err()
            .map(|err| format!("{err:?}"));
        assert!(
            err.as_deref().is_some_and(|err| err.contains("hash tag")),
            "{err:?}"
        );
    }

    #[subscriber(RedisStream::new("{orders}").group("billing"))]
    async fn cross_slot(order: &Order, Ctx(pipeline): Ctx<keys::Pipeline>) -> HandlerOutcome {
        if pipeline
            .lpush("invoices", order.id.to_string())
            .await
            .is_err()
        {
            return HandlerOutcome::retry();
        }
        HandlerOutcome::ack()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_block_across_two_slots_commits_nothing() -> Result<(), Box<dyn Error>> {
        let app = RustStream::new(AppInfo::new("shop", "0.1.0")).with_broker(
            RedisBroker::cluster(["redis-1.internal:6379"]),
            |b| {
                b.include(cross_slot.pipeline().atomic());
            },
        );
        let tb = TestApp::start(app).await?;

        tb.broker::<RedisBroker>()
            .message(&Order { id: 4 })
            .to("{orders}")
            .publish()
            .await?;

        tb.broker::<RedisBroker>()
            .published::<u64>("invoices")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }
}
