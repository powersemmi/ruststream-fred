//! The service of `examples/redis_asyncapi.rs` without the macros: the handlers are named types
//! and `main` is written by hand.
//!
//! ```text
//! cargo run --example manual_redis_asyncapi --features asyncapi -- asyncapi gen
//! cargo test --example manual_redis_asyncapi --features asyncapi
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::env;
use std::future::{Future, ready};
use std::process::ExitCode;
use std::time::Duration;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream_fred::prelude::*;

#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
}

#[derive(Debug, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Receipt {
    id: u64,
}

// `#[derive(Outgoing)]` by hand: a receipt names no destination, so each mount site does.
impl OutgoingDestination for Receipt {
    type Form = CallerName;
}

impl MessageHeaders for Receipt {
    type Contract = NoHeaders;
}

struct Confirm;

impl Handle<Order, Receipt> for Confirm {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Receipt, HandlerOutcome>> {
        ready(Ok(Receipt { id: order.id }))
    }
}

struct Log(&'static str);

impl Handle<Order> for Log {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, what = self.0, "order seen");
        ready(Ok(()))
    }
}

// --8<-- [start:channels]
/// A claiming read in the group `workers`, as the consumer `worker-1`.
fn orders() -> RedisStream {
    RedisStream::claiming("orders", Duration::from_secs(30))
        .group("workers")
        .consumer("worker-1")
}

fn jobs() -> RedisList {
    RedisList::new("jobs").reliable()
}

fn events() -> RedisPubSub {
    RedisPubSub::new("events").mode(PubSubMode::Sharded)
}

fn audit() -> RedisPubSubPattern {
    RedisPubSubPattern::new("audit.*")
}
// --8<-- [end:channels]

// --8<-- [start:server]
/// The server at `redis.internal`, database 2, logged in with a password in the URL.
fn broker(password: &str) -> RedisBroker {
    RedisBroker::standalone(format!("redis://worker:{password}@redis.internal:6379/2"))
}
// --8<-- [end:server]

// --8<-- [start:app]
// --8<-- [start:operations]
fn service(broker: RedisBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("orders", "1.0.0")).with_broker_labeled("redis", broker, |b| {
        b.include(
            subscriber(orders(), Confirm)
                .reply()
                .to("orders.done")
                .build(),
        )
        .out_reply(pubsub::Publish::new());
        b.include(subscriber(jobs(), Confirm).reply().to("jobs.done").build())
            .out_reply(list::Publish::new().ttl(Duration::from_mins(1)));
        b.include(subscriber(events(), Log("event")).build());
        b.include(subscriber(audit(), Log("audit")).build());
    })
}
// --8<-- [end:operations]

fn app() -> impl App<State = ()> {
    let password = env::var("REDIS_PASSWORD").unwrap_or_default();
    service(broker(&password))
}
// --8<-- [end:app]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::asyncapi::build_spec;
    use serde_json::{Value, json};

    use super::{broker, service};

    const PASSWORD: &str = "hunter2";

    fn document() -> Result<Value, Box<dyn Error>> {
        Ok(serde_json::from_str(
            &build_spec(&service(broker(PASSWORD))).to_json()?,
        )?)
    }

    /// The extension body of the channel named `name`.
    fn extension(document: &Value, name: &str) -> Value {
        document["channels"][name]["bindings"]["x-ruststream-redis"].clone()
    }

    #[test]
    fn the_server_is_the_address_clients_dial() -> Result<(), Box<dyn Error>> {
        let spec = document()?;

        assert_eq!(
            spec["servers"]["redis"],
            json!({ "host": "redis.internal:6379", "protocol": "redis" })
        );
        Ok(())
    }

    #[test]
    fn the_document_carries_no_credential() -> Result<(), Box<dyn Error>> {
        let text = document()?.to_string();

        assert!(!text.contains(PASSWORD), "{text}");
        Ok(())
    }

    #[test]
    fn each_subscription_reports_how_it_reads() -> Result<(), Box<dyn Error>> {
        let spec = document()?;

        assert_eq!(
            extension(&spec, "orders"),
            json!({
                "form": "stream",
                "group": "workers",
                "consumer": "worker-1",
                "readMode": "claiming",
                "minIdleMs": 30_000,
            })
        );
        assert_eq!(
            extension(&spec, "jobs"),
            json!({
                "form": "list",
                "reliable": true,
                "processing": "jobs.processing",
                "envelope": { "contentType": "application/octet-stream" },
            })
        );
        assert_eq!(
            extension(&spec, "events"),
            json!({
                "form": "pubsub",
                "mode": "sharded",
                "pattern": false,
                "envelope": { "contentType": "application/octet-stream" },
            })
        );
        assert_eq!(extension(&spec, "audit.*")["pattern"], true);
        Ok(())
    }

    #[test]
    fn each_publisher_reports_where_it_lands() -> Result<(), Box<dyn Error>> {
        let spec = document()?;

        assert_eq!(
            extension(&spec, "orders.done"),
            json!({
                "form": "pubsub",
                "channel": "orders.done",
                "mode": "classic",
                "envelope": { "contentType": "application/octet-stream" },
            })
        );
        assert_eq!(
            extension(&spec, "jobs.done"),
            json!({
                "form": "list",
                "key": "jobs.done",
                "ttlMs": 60_000,
                "envelope": { "contentType": "application/octet-stream" },
            })
        );
        Ok(())
    }
}
