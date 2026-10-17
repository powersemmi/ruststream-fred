//! The service of `examples/redis_publishing_pubsub.rs` written without `#[subscriber]`.
//!
//! ```text
//! cargo test --example manual_redis_publishing_pubsub --features testing
//! cargo run --example manual_redis_publishing_pubsub -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_fred::pubsub::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "events")]
struct Event {
    kind: String,
}

// --8<-- [start:pubsub_publish]
/// The channel an audit entry goes to is a property of the type.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "audit")]
struct AuditEntry {
    kind: String,
}

struct OnEvent;

impl Handle<Event, AuditEntry> for OnEvent {
    fn handle(
        &self,
        event: &Event,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<AuditEntry, HandlerOutcome>> {
        ready(Ok(AuditEntry {
            kind: event.kind.clone(),
        }))
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("events", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            let on_event = subscriber(RedisPubSub::new("events"), OnEvent);
            // A `PUBLISH` to the `audit` channel, in place of the default publisher's `XADD`.
            b.include(on_event.reply().build())
                .out_reply(Publish::new());
        },
    )
}
// --8<-- [end:pubsub_publish]

fn main() -> ExitCode {
    run_main(app)
}

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
