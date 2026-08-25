//! Wiring: collect the `orders` handlers into one `Router`, mounted by `main` via `include_router`.
//!
//! Keeping registration in its own module lets the handlers stay broker-agnostic - the router binds
//! to a concrete broker only when `main` mounts it.

// `RouterDef` names the opaque return type of a router builder, so it is spelled out here; the
// rest of the wiring arrives with the broker.
use ruststream::runtime::RouterDef;
use ruststream_fred::prelude::*;

use crate::orders;

/// Builds the orders router: a publishing handler (replies to the `confirmations` stream via `XADD`)
/// plus a plain one.
///
/// Every handler form mounts through `include`; a reply publisher is attached to the registration
/// it belongs to by chaining `publisher`, which also commits it back into the router.
///
/// `confirm` needs a publisher for its reply; `TypedPublisher::new` pairs the stream publish policy
/// with the default codec, reused to decode the order. The policy holds no connection, so the router
/// needs no broker handle: the runtime pairs it once the broker is connected. `on_cancel` has no
/// reply, so its `include` stands alone. The router is a consuming builder, so the calls chain;
/// the registration list is opaque, hence `impl RouterDef`.
pub fn orders() -> impl RouterDef<RedisBroker> {
    let confirmations = TypedPublisher::new(RedisPublish);

    Router::new()
        .include(orders::confirm)
        .publisher(confirmations)
        .include(orders::on_cancel)
}
