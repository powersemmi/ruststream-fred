//! The partition key: a per-message option on this crate's publishers, set by a step on the
//! publish builder.
//!
//! Redis has no partition of its own, so the resolved key travels as the well-known
//! [`PARTITION_KEY_HEADER`] header - the wire form the consumer side's
//! [`Partitioned`](ruststream::Partitioned) reads back, on all three transports.

use ruststream::HeaderMap;
use ruststream::runtime::{PublishBuilder, PublishSink};

use crate::message::PARTITION_KEY_HEADER;

/// The per-message settings of every publisher and transaction in this crate.
///
/// One field, the partition key, and it is optional like every field of an options type: a publish
/// that names no step carries no options at all, and nothing is written into its headers.
///
/// The type is the bound that keeps this crate's builder steps off another broker's publisher, and
/// it is what a handler body names when it adjusts a setting
/// (`Out<impl Publisher<Options = RedisPublishOptions>, Marker>`). A test reads the value back off
/// the slot view with `with_options`.
///
/// # Examples
///
/// ```
/// # use std::error::Error;
/// # #[cfg(feature = "testing")]
/// # mod demo {
/// use std::error::Error;
///
/// use ruststream::testing::TestApp;
/// use ruststream_fred::stream::prelude::*;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Deserialize, Serialize, Outgoing)]
/// struct Order {
///     tenant: String,
///     id: u64,
/// }
///
/// #[derive(OutSlot)]
/// #[publishes(Order)]
/// struct Ledger;
///
/// #[subscriber(RedisStream::new("orders.in").group("workers"))]
/// async fn forward(
///     order: &Order,
///     Out(ledger): Out<impl Publisher<Options = RedisPublishOptions>, Ledger>,
/// ) -> HandlerOutcome {
///     let sent = ledger
///         .message(order)
///         .to("orders.keyed")
///         .partition_key(&order.tenant)
///         .publish()
///         .await;
///     match sent {
///         Ok(_) => HandlerOutcome::ack(),
///         Err(_) => HandlerOutcome::retry(),
///     }
/// }
///
/// pub fn app() -> impl App<State = ()> {
///     RustStream::new(AppInfo::new("orders", "0.1.0"))
///         .with_broker(RedisBroker::standalone("redis://localhost:6379"), |b| {
///             b.include(forward).out(Ledger, Publish).build();
///         })
/// }
///
/// pub async fn the_ledger_is_keyed_by_tenant() -> Result<(), Box<dyn Error>> {
///     let tb = TestApp::start(app()).await?;
///     let order = Order {
///         tenant: "tenant-a".to_owned(),
///         id: 7,
///     };
///     tb.broker::<RedisBroker>()
///         .message(&order)
///         .to("orders.in")
///         .publish()
///         .await?;
///
///     tb.out::<Ledger>()
///         .assert_called_once()
///         .with_options(&RedisPublishOptions {
///             partition_key: Some(b"tenant-a".to_vec()),
///             ..RedisPublishOptions::default()
///         });
///     Ok(())
/// }
/// # }
/// # #[cfg(feature = "testing")]
/// # fn main() -> Result<(), Box<dyn Error>> {
/// #     tokio::runtime::Runtime::new()?.block_on(demo::the_ledger_is_keyed_by_tenant())
/// # }
/// # #[cfg(not(feature = "testing"))]
/// # fn main() {}
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RedisPublishOptions {
    /// The key this one message is partitioned by, or `None` to send it unkeyed.
    ///
    /// Opaque bytes: the runtime hashes them to pick a dispatch lane, and never interprets them.
    pub partition_key: Option<Vec<u8>>,

    /// Whether this message joins the round of the delivery being handled, on a `.pipeline()`
    /// subscription, instead of leaving at once.
    ///
    /// The [`InRound`](crate::pipeline::InRound) transform sets it on a reply; a message sent
    /// outside a pipelined delivery's handling leaves at once whatever it says.
    pub join_round: bool,
}

/// The steps this crate adds to the publish builder.
///
/// Import it from any of this crate's preludes to reach [`partition_key`](Self::partition_key) on
/// a publish over a Redis publisher. The impl is bounded on [`RedisPublishOptions`], so the step
/// does not appear on a builder over another broker's publisher.
///
/// # Examples
///
/// ```
/// # mod demo {
/// use ruststream_fred::stream::prelude::*;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Deserialize, Serialize, Outgoing)]
/// #[outgoing(name = "orders")]
/// struct Order {
///     tenant: String,
///     id: u64,
/// }
///
/// #[derive(OutSlot)]
/// #[publishes(Order)]
/// struct Orders;
///
/// // Every order of one tenant lands in the same `workers(n, by_key)` lane downstream, so their
/// // relative order is preserved while other tenants run in parallel.
/// #[subscriber(RedisStream::new("orders.in").group("intake"))]
/// async fn intake(
///     order: &Order,
///     Out(orders): Out<impl Publisher<Options = RedisPublishOptions>, Orders>,
/// ) -> HandlerOutcome {
///     match orders.message(order).partition_key(&order.tenant).publish().await {
///         Ok(_) => HandlerOutcome::ack(),
///         Err(_) => HandlerOutcome::retry(),
///     }
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
///         RedisBroker::standalone("redis://localhost:6379"),
///         |b| {
///             b.include(intake).out(Orders, Publish).build();
///         },
///     )
/// }
/// # }
/// # fn main() {}
/// ```
pub trait RedisPublishSteps {
    /// Sends this one message under `key`, whatever the rest of the chain says.
    ///
    /// The key feeds the runtime's keyed worker lanes (`workers(n, by_key)`): deliveries sharing a
    /// key are dispatched to the same lane, so their relative order survives concurrency. It
    /// reaches the consumer as the [`PARTITION_KEY_HEADER`](crate::PARTITION_KEY_HEADER) header,
    /// written over whatever the call site itself put under that name.
    #[must_use]
    fn partition_key(self, key: impl AsRef<[u8]>) -> Self;
}

impl<Sink, Body, Enc, Hdrs, Dest> RedisPublishSteps for PublishBuilder<Sink, Body, Enc, Hdrs, Dest>
where
    Sink: PublishSink<Options = RedisPublishOptions>,
{
    fn partition_key(mut self, key: impl AsRef<[u8]>) -> Self {
        self.options_mut()
            .get_or_insert_with(RedisPublishOptions::default)
            .partition_key = Some(key.as_ref().to_vec());
        self
    }
}

/// The headers a publish leaves with, once `options` are resolved over the ones it carries.
///
/// A step wins over the header the call site wrote itself, because it names this one message while
/// the header map may be a contract the message type declares. A publish no step touched keeps its
/// headers untouched, so writing [`PARTITION_KEY_HEADER`] by hand stays the portable spelling.
/// The map arrives owned because a publish owns it: it is what the transforms filled, taken out
/// of the outgoing message, so resolving a step writes into it instead of copying it.
pub(crate) fn resolved_headers(
    mut headers: HeaderMap,
    options: Option<&RedisPublishOptions>,
) -> HeaderMap {
    if let Some(key) = options.and_then(|options| options.partition_key.as_deref()) {
        headers.insert(PARTITION_KEY_HEADER, key.to_vec());
    }
    headers
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_unstepped_publish_keeps_its_own_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(PARTITION_KEY_HEADER, "call-site");

        let resolved = resolved_headers(headers, None);
        assert_eq!(
            resolved.get(PARTITION_KEY_HEADER),
            Some(b"call-site".as_slice())
        );
    }

    #[test]
    fn a_step_writes_over_the_call_sites_own_header() {
        let mut headers = HeaderMap::new();
        headers.insert(PARTITION_KEY_HEADER, "call-site");
        headers.insert("trace-id", "abc");

        let options = RedisPublishOptions {
            partition_key: Some(b"tenant-a".to_vec()),
            ..RedisPublishOptions::default()
        };
        let resolved = resolved_headers(headers, Some(&options));

        assert_eq!(
            resolved.get(PARTITION_KEY_HEADER),
            Some(b"tenant-a".as_slice())
        );
        // Unrelated entries travel untouched next to the key.
        assert_eq!(resolved.get("trace-id"), Some(b"abc".as_slice()));
    }
}
