//! The service of `examples/redis_subscriptions_envelopes.rs` written without the macros: the
//! broker's envelope, one a descriptor or a policy names instead, and an envelope of the service's
//! own.
//!
//! ```text
//! cargo test --example manual_redis_subscriptions_envelopes --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::collections::BTreeMap;
use std::future::{Future, ready};

use ruststream::codec::JsonCodec;
use ruststream::schemars::{self, JsonSchema};
use ruststream_fred::envelope::{Envelope, Location, SealError};
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

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
struct LegacyJob {
    order: u64,
}

// `#[outgoing(name = "legacy.jobs")]` by hand.
impl OutgoingDestination for LegacyJob {
    type Form = FixedName;

    const DESTINATION: &'static str = "legacy.jobs";
}

impl MessageHeaders for LegacyJob {
    type Contract = NoHeaders;
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
struct Price {
    sku: String,
    cents: u64,
}

// `#[outgoing(name = "prices")]` by hand.
impl OutgoingDestination for Price {
    type Form = FixedName;

    const DESTINATION: &'static str = "prices";
}

impl MessageHeaders for Price {
    type Contract = NoHeaders;
}

// --8<-- [start:envelopes]
/// The list jobs leave through.
struct Jobs;

impl OutSlot for Jobs {
    const NAME: &'static str = "Jobs";
    // `Job` fixes its destination, so no transform on this slot may name one.
    type Destination = Reads;
}

impl PublishedThrough<Jobs> for Job {}

/// Reads the orders a faststream shop appends, in the broker's envelope, and queues a job for
/// each with the tenant in a header.
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

/// Runs each job for the tenant its header names; the list holds JSON documents.
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

/// Reads the prices a feed publishes as bare JSON, with no headers.
struct OnPrice;

impl Handle<Price> for OnPrice {
    fn handle(
        &self,
        price: &Price,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(price.sku, price.cents, "price");
        ready(Ok(()))
    }
}

fn app() -> impl App {
    // faststream's frame wherever nothing names another envelope.
    let broker = RedisBroker::standalone("redis://localhost:6379").envelope(FastStream);
    RustStream::new(AppInfo::new("jobs", "0.1.0")).with_broker(broker, |b| {
        let orders = RedisStream::new("orders").group("dispatch");
        // Both ends of `jobs` name JSON, so `redis-cli` and RedisInsight show each job as text.
        b.include(subscriber(orders, Dispatch).build())
            .out(
                Jobs,
                RedisListPublish::new().envelope(Framed::new(JsonCodec)),
            )
            .build();
        let jobs = RedisList::new("jobs").envelope(Framed::new(JsonCodec));
        b.include(subscriber(jobs, Run).build());
        let prices = RedisPubSub::new("prices").envelope(NoOp);
        b.include(subscriber(prices, OnPrice).build());
    })
}
// --8<-- [end:envelopes]

// --8<-- [start:custom]
/// One JSON line with the headers under `meta` and the payload as text under `body`: the layout
/// a legacy system on the same list writes.
#[derive(Debug, Clone, Copy, Default)]
struct MetaLine;

#[derive(Serialize, Deserialize)]
struct Line {
    meta: BTreeMap<String, String>,
    body: String,
}

impl Envelope for MetaLine {
    const CONTENT_TYPE: &'static str = "application/json";

    fn locate(&self, value: &[u8]) -> Location {
        serde_json::from_slice::<Line>(value).map_or(Location::Bare, |line| {
            Location::Decoded(line.body.into_bytes())
        })
    }

    fn headers(&self, value: &[u8]) -> Option<HeaderMap> {
        let line = serde_json::from_slice::<Line>(value).ok()?;
        let mut headers = HeaderMap::new();
        for (name, text) in line.meta {
            headers.insert(name, text);
        }
        Some(headers)
    }

    fn seal(
        &self,
        payload: &[u8],
        headers: &HeaderMap,
        value: &mut Vec<u8>,
    ) -> Result<(), SealError> {
        let line = Line {
            meta: headers
                .iter()
                .map(|(name, text)| (name.to_owned(), String::from_utf8_lossy(text).into_owned()))
                .collect(),
            body: String::from_utf8_lossy(payload).into_owned(),
        };
        serde_json::to_writer(value, &line).map_err(|err| SealError::Encode(Box::new(err)))
    }
}

/// Reads the legacy system's lines, the tenant from `meta`.
struct Legacy;

impl Handle<LegacyJob> for Legacy {
    fn handle(
        &self,
        job: &LegacyJob,
        _outs: &(),
        ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        let tenant = ctx.headers().get_str("tenant").unwrap_or("unknown");
        tracing::info!(job.order, tenant, "legacy job run");
        ready(Ok(()))
    }
}

fn legacy_app() -> impl App {
    RustStream::new(AppInfo::new("legacy", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            let jobs = RedisList::new("legacy.jobs").envelope(MetaLine);
            b.include(subscriber(jobs, Legacy).build());
        },
    )
}
// --8<-- [end:custom]

// The scenarios are the tests below; the command in the header runs them.
fn main() {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;

    use super::*;

    /// An order in faststream's frame becomes a job, and the tenant header leaves with the job in
    /// its JSON document and reaches the handler on the list.
    #[tokio::test]
    async fn the_envelope_carries_the_header_to_the_list_handler() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<RedisBroker<FastStream>>();

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

    /// A price published with no envelope reaches the handler as it is.
    #[tokio::test]
    async fn a_bare_price_reaches_the_handler() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<RedisBroker<FastStream>>();
        let tea = Price {
            sku: "tea".to_owned(),
            cents: 450,
        };

        broker.message(&tea).publish().await?;

        broker
            .subscriber("prices")
            .assert_called_once()
            .with(&tea)
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    /// A job written as the legacy system writes it, with its tenant in `meta`, is read through
    /// the envelope of the service's own.
    #[tokio::test]
    async fn a_legacy_line_reaches_the_handler() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(legacy_app()).await?;
        let broker = tb.broker::<RedisBroker>();
        let mut headers = HeaderMap::new();
        headers.insert("tenant", "acme");

        broker
            .message(&LegacyJob { order: 7 })
            .with_headers(headers)
            .publish()
            .await?;

        broker
            .subscriber("legacy.jobs")
            .assert_called_once()
            .with(&LegacyJob { order: 7 })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
}
