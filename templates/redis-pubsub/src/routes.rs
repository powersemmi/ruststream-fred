//! Wiring: collect the handlers into one `Router`, mounted by `main` via `include_router`.
//!
//! Keeping registration in its own module lets the handlers stay broker-agnostic - the router binds
//! to a concrete broker only when `main` mounts it.

// `RouterDef` names the opaque return type of a router builder, so it is spelled out here; the
// rest of the wiring arrives with the broker.
use ruststream::runtime::RouterDef;
use ruststream_fred::pubsub::prelude::*;

use crate::orders;

/// Builds the events router. Pub/Sub is consume-only (no acknowledgement, so no reply publisher),
/// hence the router takes no broker handle: it just mounts `on_event` with `include`. The
/// registration list is opaque, hence `impl RouterDef`.
pub fn events() -> impl RouterDef<RedisBroker> {
    Router::new().include(orders::on_event)
}
