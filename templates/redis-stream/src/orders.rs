//! Domain types and handlers, written as `#[subscriber]` functions.
//!
//! The first parameter is the decoded payload; the macro turns each function into a mountable
//! definition (a value named after the function) that `routes` collects into a `Router`. Both
//! handlers bind to a [`RedisStream`] descriptor naming the `workers` consumer group, so delivery is
//! durable and each entry is `XACK`ed when the handler returns `Ack`. `confirm` consumes `orders` and
//! replies on the `confirmations` stream; `on_cancel` consumes `cancellations`.

use ruststream::runtime::HandlerResult;
use ruststream::subscriber;
use ruststream_fred::RedisStream;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// An order placed on the `orders` stream.
///
/// `JsonSchema` lets `asyncapi gen` emit this payload's schema into the generated document.
#[derive(Debug, Deserialize, JsonSchema)]
pub struct Order {
    pub id: u64,
    pub item: String,
    pub quantity: u32,
}

/// The reply published to the `confirmations` stream for each order.
#[derive(Debug, Serialize, JsonSchema)]
pub struct Confirmation {
    pub id: u64,
    pub accepted: bool,
}

/// Confirms an incoming order and publishes a `Confirmation` to the `confirmations` stream.
///
/// The subscription reads through the `workers` consumer group, so it is durable: the entry is
/// `XACK`ed once this returns. The `publish("confirmations")` clause makes the runtime encode the
/// return value and `XADD` it through the publisher wired in `routes`.
#[subscriber(RedisStream::new("orders").group("workers"), publish("confirmations"))]
pub async fn confirm(order: &Order) -> Confirmation {
    Confirmation {
        id: order.id,
        accepted: order.quantity > 0,
    }
}

/// Logs cancellations read through the same group. No reply, so it returns a plain `HandlerResult`;
/// `Ack` triggers the `XACK`.
#[subscriber(RedisStream::new("cancellations").group("workers"))]
pub async fn on_cancel(order: &Order) -> HandlerResult {
    println!("order {} ({}) cancelled", order.id, order.item);
    HandlerResult::Ack
}
