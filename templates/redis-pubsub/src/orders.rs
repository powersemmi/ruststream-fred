//! Domain types and handlers, written as `#[subscriber]` functions.
//!
//! The first parameter is the decoded payload; the macro turns each function into a mountable
//! definition (a value named after the function) that `routes` collects into a `Router`. `on_event`
//! binds to a [`RedisPubSub`] channel and just consumes: Pub/Sub has no acknowledgement, so there is
//! no reply path here (its `ack` / `nack` report `Unsupported`). Returning `Ack` simply marks the
//! delivery handled; the runtime tolerates the unsupported settle.

use ruststream::runtime::HandlerResult;
use ruststream::subscriber;
use ruststream_fred::RedisPubSub;
use schemars::JsonSchema;
use serde::Deserialize;

/// An event broadcast on the `events` channel.
///
/// `JsonSchema` lets `asyncapi gen` emit this payload's schema into the generated document.
#[derive(Debug, Deserialize, JsonSchema)]
pub struct Event {
    pub kind: String,
}

/// Consumes each `events` broadcast. Pub/Sub fans out to every connected subscriber and cannot ack,
/// so this is a read-only handler: it does its work and returns `Ack` to mark the delivery handled.
#[subscriber(RedisPubSub::new("events"))]
pub async fn on_event(event: &Event) -> HandlerResult {
    println!("event: {}", event.kind);
    HandlerResult::Ack
}
