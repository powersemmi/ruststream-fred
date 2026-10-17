//! The service of `examples/redis_subscriptions_framing.rs` written without the macros: the
//! envelope codec is a setting of the list descriptor and of the list publish policy.
//!
//! ```text
//! cargo test --example manual_redis_subscriptions_framing --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::future::{Future, ready};

use ruststream::codec::JsonCodec;
use ruststream::schemars::{self, JsonSchema};
use ruststream_fred::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
struct Order {
    id: u64,
    tenant: String,
}

// `#[outgoing(name = "orders")]` by hand: the type fixes its destination.
impl OutgoingDestination for Order {
    type Form = FixedName;

    const DESTINATION: &'static str = "orders";
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
struct Job {
    order: u64,
}

// `#[outgoing(name = "jobs")]` by hand.
impl OutgoingDestination for Job {
    type Form = FixedName;

    const DESTINATION: &'static str = "jobs";
}

impl MessageHeaders for Job {
    type Contract = NoHeaders;
}

impl MessageInfo for Job {
    const NAME: &'static str = "Job";
}

// --8<-- [start:framing]
/// The list jobs leave through.
struct Jobs;

impl OutSlot for Jobs {
    const NAME: &'static str = "Jobs";
    // `Job` fixes its destination, so no transform on this slot may name one.
    type Destination = Reads;
}

impl PublishedThrough<Jobs> for Job {}

/// Queues a job for each order, with the tenant in a header.
struct Dispatch;

impl<Entry> Handle<Order, (), Outs<(Entry,)>> for Dispatch
where
    Entry: OutEntry<Jobs, Wire: Publisher>,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(Entry,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let mut headers = HeaderMap::new();
        headers.insert("tenant", order.tenant.clone());
        let job = Job { order: order.id };
        outs.get(Jobs)
            .message(&job)
            .with_headers(headers)
            .publish()
            .await
            .map_err(|_| HandlerOutcome::retry())
    }
}

/// Runs each job for the tenant its header names.
struct Run;

impl Handle<Job> for Run {
    fn handle(
        &self,
        job: &Job,
        _outs: &(),
        ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        let Some(tenant) = ctx.headers().get_str("tenant") else {
            tracing::warn!(job.order, "a job without a tenant is dropped");
            return ready(Err(HandlerOutcome::drop()));
        };
        tracing::info!(job.order, tenant, "job run");
        ready(Ok(()))
    }
}

fn app() -> impl App {
    let broker = RedisBroker::standalone("redis://localhost:6379");
    RustStream::new(AppInfo::new("jobs", "0.1.0")).with_broker(broker, |b| {
        // Both ends name JSON, so `redis-cli` and RedisInsight show each entry as text.
        let orders = RedisStream::new("orders").group("dispatch");
        b.include(subscriber(orders, Dispatch).build())
            .out(Jobs, RedisListPublish::new().codec(JsonCodec))
            .build();
        let jobs = RedisList::new("jobs").codec(JsonCodec);
        b.include(subscriber(jobs, Run).build());
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
