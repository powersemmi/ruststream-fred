//! `RedisPubSubPublish`, `Publish` in the Pub/Sub prelude: a reply sent with `PUBLISH` to every
//! subscriber of its channel.
//!
//! ```text
//! cargo test --example redis_publishing_pubsub --features testing
//! cargo run --example redis_publishing_pubsub -- run
//! ```

use ruststream_fred::pubsub::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "events")]
struct Event {
    kind: String,
}

// --8<-- [start:pubsub_publish]
/// The channel an audit entry goes to is a property of the type.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "audit")]
struct AuditEntry {
    kind: String,
}

#[subscriber(RedisPubSub::new("events"), reply)]
async fn on_event(event: &Event) -> AuditEntry {
    AuditEntry {
        kind: event.kind.clone(),
    }
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("events", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            // A `PUBLISH` to the `audit` channel, in place of the default publisher's `XADD`.
            b.include(on_event).out_reply(Publish::new());
        },
    )
}
// --8<-- [end:pubsub_publish]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_fred::pubsub::prelude::*;

    use super::{AuditEntry, Event, app};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_event_publishes_its_audit_entry() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<RedisBroker>()
            .message(&Event {
                kind: "created".to_owned(),
            })
            .publish()
            .await?;

        tb.broker::<RedisBroker>()
            .published::<AuditEntry>("audit")
            .assert_called_once()
            .with(&AuditEntry {
                kind: "created".to_owned(),
            });
        tb.shutdown().await?;
        Ok(())
    }
}
