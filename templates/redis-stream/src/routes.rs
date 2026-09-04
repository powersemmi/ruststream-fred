//! Wiring: collect the `orders` handlers into one `Router`, mounted by `main` via `include_router`.
//!
//! Keeping registration in its own module lets the handlers stay broker-agnostic - the router binds
//! to a concrete broker only when `main` mounts it.

// `RouterDef` names the opaque return type of a router builder, so it is spelled out here; the
// rest of the wiring arrives with the broker.
use ruststream::runtime::RouterDef;
use ruststream_fred::stream::prelude::*;

use crate::orders;

/// Builds the orders router: a publishing handler (replies to the `confirmations` stream via `XADD`)
/// plus a plain one.
///
/// Every handler form mounts through `include`; a reply publisher is attached to the registration
/// it belongs to by chaining `publisher`, and the attachment closes with `build`, which commits it
/// back into the router.
///
/// `confirm` needs a publisher for its reply, so its `include` names this form's `Publish` policy
/// and the reply leaves under the default codec. The policy holds no connection, so the router needs
/// no broker handle: the runtime pairs it once the broker is connected. `on_cancel` has no reply, so
/// its `include` stands alone. The router is a consuming builder, so the calls chain; the
/// registration list is opaque, hence `impl RouterDef`.
pub fn orders() -> impl RouterDef<RedisBroker> {
    Router::new()
        .include(orders::confirm)
        .publisher(Publish)
        .build()
        .include(orders::on_cancel)
}
