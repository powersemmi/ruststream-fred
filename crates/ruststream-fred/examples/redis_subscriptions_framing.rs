//! Headers around a list entry or a channel message: a binary frame by default, or a readable
//! envelope when both ends set one codec. Each scenario is a test.
//!
//! ```text
//! cargo test --example redis_subscriptions_framing --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream::codec::JsonCodec;
use ruststream_fred::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
    tenant: String,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "jobs")]
struct Job {
    order: u64,
}

// --8<-- [start:framing]
/// The list jobs leave through.
#[derive(OutSlot)]
#[publishes(Job)]
struct Jobs;

/// Queues a job for each order, with the tenant in a header.
#[subscriber(RedisStream::new("orders").group("dispatch"))]
async fn dispatch(order: &Order, Out(jobs): Out<impl Publisher, Jobs>) -> HandlerOutcome {
    let mut headers = HeaderMap::new();
    headers.insert("tenant", order.tenant.clone());
    let job = Job { order: order.id };
    if jobs
        .message(&job)
        .with_headers(headers)
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

/// Runs each job for the tenant its header names.
#[subscriber(RedisList::new("jobs").codec(JsonCodec))]
async fn run(job: &Job, ctx: &mut Context<'_>) -> HandlerOutcome {
    let Some(tenant) = ctx.headers().get_str("tenant") else {
        tracing::warn!(job.order, "a job without a tenant is dropped");
        return HandlerOutcome::drop();
    };
    tracing::info!(job.order, tenant, "job run");
    HandlerOutcome::ack()
}

fn app() -> impl App {
    let broker = RedisBroker::standalone("redis://localhost:6379");
    RustStream::new(AppInfo::new("jobs", "0.1.0")).with_broker(broker, |b| {
        // Both ends name JSON, so `redis-cli` and RedisInsight show each entry as text.
        b.include(dispatch)
            .out(Jobs, RedisListPublish::new().codec(JsonCodec))
            .build();
        b.include(run);
    })
}
// --8<-- [end:framing]

// The scenarios are the tests below; the command in the header runs them.
fn main() {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;

    use super::*;

    /// The tenant header leaves with the job in its envelope and reaches the handler on the list.
    #[tokio::test]
    async fn the_envelope_carries_the_header_to_the_list_handler() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<RedisBroker>();

        broker
            .message(&Order {
                id: 1,
                tenant: "acme".to_owned(),
            })
            .publish()
            .await?;

        broker
            .subscriber("jobs")
            .assert_called_once()
            .with(&Job { order: 1 })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
}
