//! The core prelude plus everything a service mixing all three Redis forms writes.
//!
//! The broker, every descriptor and publish policy, the seek types, the per-delivery and
//! batch contexts with their [`keys`], and the [`crate::stream`], [`crate::list`] and
//! [`crate::pubsub`] modules.
//!
//! # Examples
//!
//! ```
//! # mod demo {
//! use ruststream_fred::prelude::*;
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Deserialize)]
//! struct Order {
//!     id: u64,
//! }
//!
//! #[derive(Serialize, Outgoing)]
//! #[outgoing(name = "orders.shipped")]
//! struct Shipped {
//!     id: u64,
//! }
//!
//! #[subscriber(RedisStream::new("orders").group("workers"), publish)]
//! async fn ship(order: &Order) -> Shipped {
//!     Shipped { id: order.id }
//! }
//!
//! #[subscriber(RedisList::new("invoices").reliable())]
//! async fn invoice(order: &Order) -> HandlerOutcome {
//!     println!("invoicing order {}", order.id);
//!     HandlerOutcome::ack()
//! }
//!
//! #[ruststream::app]
//! fn app() -> impl App {
//!     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
//!         RedisBroker::standalone("redis://localhost:6379"),
//!         |b| {
//!             // A stream in, a Pub/Sub broadcast out: the form prefix says which `Publish`.
//!             b.include(ship).out_reply(pubsub::Publish::default());
//!             b.include(invoice);
//!         },
//!     )
//! }
//! # }
//! # fn main() {}
//! ```
//!
//! A service on a single form globs that form's prelude instead, which carries `Publish` under the
//! uniform mount-site name and only the capabilities that form has.

pub use ruststream::prelude::*;

// Do not add `Partitioned` here: the core surfaces `partition_key` through `IncomingMessage`'s
// defaulted method, which this glob already carries, so re-exporting the trait makes the call
// ambiguous (E0034).
pub use ruststream::{OwnedTransactions, Positioned, Seeker, Transaction, TransactionalPublisher};

// `keys` arrives as the module, not as a glob: its members are short words a service also uses for
// its own types, and `Ctx<keys::SeekHandle>` reads as what it is at the use site.
pub use crate::context::{
    PipelineContext, PoolContext, PubSubContext, StreamBatchContext, StreamContext, keys,
};

pub use crate::pipeline::{Bindable, InRound, RedisPipelineSteps};

pub use crate::{
    DelayedRetry, PARTITION_KEY_HEADER, PubSubMode, RedisBroker, RedisGroupPosition,
    RedisGroupSeeker, RedisList, RedisListPublish, RedisPubSub, RedisPubSubPattern,
    RedisPubSubPublish, RedisPublish, RedisPublishOptions, RedisPublishSteps, RedisStream,
    RedisSubscribeExt, StreamStart,
};

// The policies keep their prefixed names here, and there is no bare `Publish`: this glob spans all
// three forms, so the one mount-site word would name three colliding types. A mixed file globs
// this prelude and writes `stream::Publish` beside `pubsub::Publish` through these modules.
pub use crate::{list, pubsub, stream};

#[cfg(any(
    feature = "tls-rustls",
    feature = "tls-rustls-ring",
    feature = "tls-native-tls"
))]
pub use crate::{TlsConfig, TlsConnector};
