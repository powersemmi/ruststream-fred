//! Wiring: collect the `orders` handlers into one `Router`, mounted by `main` via `include_router`.
//!
//! Keeping registration in its own module lets the handlers stay broker-agnostic - the router binds
//! to a concrete broker only when `main` mounts it.

use ruststream::runtime::{Router, RouterDef, TypedPublisher};
use ruststream_fred::{RedisBroker, RedisPublish};

use crate::orders;

/// Builds the orders router: a publishing handler (replies to the `confirmations` stream via `XADD`)
/// plus a plain one.
///
/// `confirm` needs a publisher for its reply; `TypedPublisher::new` pairs the stream publish policy
/// with the default codec, reused to decode the order. The policy holds no connection, so the router
/// needs no broker handle: the runtime pairs it once the broker is connected. `on_cancel` has no
/// reply, so it is mounted with `include`. The router is a consuming builder, so the calls chain;
/// the registration list is opaque, hence `impl RouterDef`.
pub fn orders() -> impl RouterDef<RedisBroker> {
    let confirmations = TypedPublisher::new(RedisPublish);

    Router::new()
        .include_publishing(orders::confirm, confirmations)
        .include(orders::on_cancel)
}
