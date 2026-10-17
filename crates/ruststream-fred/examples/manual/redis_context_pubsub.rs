//! The service of `examples/redis_context_pubsub.rs` written without `#[subscriber]`: a body
//! typed over `PubSubContext` reads the channel and the pattern flag off its context.
//!
//! ```text
//! cargo test --example manual_redis_context_pubsub --features testing
//! cargo run --example manual_redis_context_pubsub -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_fred::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Event {
    id: u64,
}

/// Which channel an event was heard on, written to a stream.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "heard")]
struct Heard {
    event_id: u64,
    channel: String,
    from_pattern: bool,
}

// --8<-- [start:pubsub]
/// Hears every `events.*` channel and records which one each event came from.
struct Hear;

impl Handle<Event, Heard, (), PubSubContext> for Hear {
    fn handle(
        &self,
        event: &Event,
        _outs: &(),
        ctx: &mut Context<'_, PubSubContext>,
    ) -> impl Future<Output = Result<Heard, HandlerOutcome>> {
        let channel = ctx.context(keys::Channel);
        tracing::info!(event.id, channel, "event heard");
        ready(Ok(Heard {
            event_id: event.id,
            channel: channel.to_owned(),
            from_pattern: ctx.context(keys::FromPattern),
        }))
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("events", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            let hear = subscriber(RedisPubSubPattern::new("events.*"), Hear);
            // A pattern names no channel a retry could go back to, so the mount names one.
            b.include(hear.reply().build())
                .out_reply(stream::Publish)
                .out_retry(pubsub::Publish::new())
                .to("events.retry");
        },
    )
}
// --8<-- [end:pubsub]

fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_fred::prelude::*;

    use super::{Event, Heard, app};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn each_event_names_its_channel() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<RedisBroker>()
            .message(&Event { id: 1 })
            .to("events.created")
            .publish()
            .await?;
        tb.broker::<RedisBroker>()
            .message(&Event { id: 2 })
            .to("events.shipped")
            .publish()
            .await?;

        let heard = tb
            .broker::<RedisBroker>()
            .published::<Heard>("heard")
            .assert_called(2)
            .decoded();
        let channels: Vec<(u64, &str, bool)> = heard
            .iter()
            .map(|heard| (heard.event_id, heard.channel.as_str(), heard.from_pattern))
            .collect();
        assert_eq!(
            channels,
            [(1, "events.created", true), (2, "events.shipped", true)],
        );
        tb.shutdown().await?;
        Ok(())
    }
}
