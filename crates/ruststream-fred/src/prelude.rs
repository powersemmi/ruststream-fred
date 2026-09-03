//! The core prelude plus everything a service mixing all three Redis forms writes.
//!
//! The broker, all three descriptors and publish policies, the seek types, the per-delivery and
//! page contexts with their [`keys`], and the [`crate::stream`], [`crate::list`] and
//! [`crate::pubsub`] modules.
//!
//! # Examples
//!
//! ```
//! use ruststream_fred::prelude::*;
//!
//! let orders = RedisStream::new("orders").group("workers");
//! let broker = RedisBroker::standalone("redis://localhost:6379");
//! let replies = TypedPublisher::new(stream::Publish);
//! let _ = (orders, broker, replies);
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
pub use crate::context::{PubSubContext, StreamBatchContext, StreamContext, keys};

pub use crate::{
    DelayedRetry, PARTITION_KEY_HEADER, PubSubMode, RedisBroker, RedisGroupPosition,
    RedisGroupSeeker, RedisList, RedisListPublish, RedisPubSub, RedisPubSubPublish, RedisPublish,
    RedisPublishExt, RedisStream, StreamStart,
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
