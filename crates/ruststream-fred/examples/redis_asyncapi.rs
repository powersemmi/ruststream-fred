//! The `AsyncAPI` document of a Redis service: the server entry says where clients connect, and
//! the `x-ruststream-redis` extension says which structure carries each channel and how. The
//! tests read the document the app generates.
//!
//! ```text
//! cargo run --example redis_asyncapi --features asyncapi -- asyncapi gen
//! cargo test --example redis_asyncapi --features asyncapi
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::env;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use ruststream_fred::prelude::*;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

#[derive(Debug, Serialize, Outgoing)]
struct Receipt {
    id: u64,
}

// --8<-- [start:channels]
#[subscriber(
    RedisStream::claiming("orders", Duration::from_secs(30))
        .group("workers")
        .consumer("worker-1"),
    reply("orders.done")
)]
async fn confirm(order: &Order) -> Receipt {
    Receipt { id: order.id }
}

#[subscriber(RedisList::new("jobs").reliable(), reply("jobs.done"))]
async fn run_job(order: &Order) -> Receipt {
    Receipt { id: order.id }
}

#[subscriber(RedisPubSub::new("events").mode(PubSubMode::Sharded))]
async fn on_event(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "event seen");
    HandlerOutcome::ack()
}

#[subscriber(RedisPubSubPattern::new("audit.*"))]
async fn audit(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "audited");
    HandlerOutcome::ack()
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
        b.include(confirm).out_reply(pubsub::Publish::new());
        b.include(run_job)
            .out_reply(list::Publish::new().ttl(Duration::from_mins(1)));
        b.include(on_event);
        b.include(audit);
    })
}
// --8<-- [end:operations]

#[ruststream::app]
fn app() -> impl App<State = ()> {
    let password = env::var("REDIS_PASSWORD").unwrap_or_default();
    service(broker(&password))
}
// --8<-- [end:app]

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
