//! Redis Pub/Sub subscribers: classic broadcast and sharded (cluster-scalable) delivery, plus
//! re-publishing from a handler with the macro `publish(...)` form.
//!
//! Pub/Sub is fire-and-forget: no durability, no consumer groups, no ack. A descriptor selects the
//! mode. Classic (`SUBSCRIBE`) broadcasts cluster-wide and supports patterns; sharded
//! (`SSUBSCRIBE`, Redis 7+) is slot-local so it scales across a cluster but has no patterns.
//!
//! ```text
//! cargo run --example fred_pubsub --features macros,json -- run
//! ```
//!
//! Publish from another terminal:
//!
//! ```text
//! redis-cli PUBLISH events '{"kind":"login"}'
//! ```

use ruststream::runtime::{AppInfo, HandlerResult, RustStream, TypedPublisher};
use ruststream::subscriber;
use ruststream_fred::{PubSubMode, RedisBroker, RedisPubSub};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
struct Event {
    kind: String,
}

// --8<-- [start:classic]
// Classic broadcast subscription that re-publishes each event to an `audit` channel. The macro
// `publish("audit")` form publishes the handler's return value through the publisher wired at mount.
#[subscriber(RedisPubSub::new("events"), publish("audit"))]
async fn on_event(event: &Event) -> Event {
    println!("event: {}", event.kind);
    Event {
        kind: event.kind.clone(),
    }
}
// --8<-- [end:classic]

// --8<-- [start:sharded]
// Sharded subscription (`SSUBSCRIBE`): on a cluster this stays slot-local and scales. It belongs on
// a cluster broker (below), and pairs with a sharded publisher
// (`broker.pubsub_publisher().mode(Sharded)`).
#[subscriber(RedisPubSub::new("events").mode(PubSubMode::Sharded))]
async fn on_event_sharded(event: &Event) -> HandlerResult {
    println!("sharded event: {}", event.kind);
    HandlerResult::Ack
}
// --8<-- [end:sharded]

// One service, two brokers: classic broadcast on a standalone server and sharded delivery on a
// cluster. RustStream wires each handler onto its own broker.
// --8<-- [start:app]
#[ruststream::app]
fn app() -> RustStream {
    RustStream::new(AppInfo::new("events", "0.1.0"))
        .with_broker(RedisBroker::standalone("redis://localhost:6379"), |b| {
            // `publish("audit")` sends through this Pub/Sub publisher (PUBLISH), not the default
            // stream publisher (XADD).
            let audit = TypedPublisher::new(b.broker().pubsub_publisher());
            b.include_publishing(on_event, audit);
        })
        .with_broker(RedisBroker::cluster(["redis://localhost:7000"]), |b| {
            b.include(on_event_sharded);
        })
}
// --8<-- [end:app]
