//! The core prelude, the broker, all three descriptors and publish policies, the seek types, and
//! the [`crate::stream`], [`crate::list`] and [`crate::pubsub`] modules.
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
//! uniform name and only the capabilities that form has.

pub use ruststream::prelude::*;

// Do not add `Partitioned` here: the core surfaces `partition_key` through `IncomingMessage`'s
// defaulted method, which this glob already carries, so re-exporting the trait makes the call
// ambiguous (E0034).
pub use ruststream::{OwnedTransactions, Positioned, Seeker, Transaction, TransactionalPublisher};

pub use crate::{
    DelayedRetry, PARTITION_KEY_HEADER, PubSubMode, RedisBroker, RedisGroupPosition,
    RedisGroupSeeker, RedisList, RedisListPublish, RedisPubSub, RedisPubSubPublish, RedisPublish,
    RedisPublishExt, RedisStream, StreamStart,
};

// No bare `Publish` at crate level: the word belongs to a form, and a mixed file needs all three.
pub use crate::{list, pubsub, stream};

#[cfg(any(
    feature = "tls-rustls",
    feature = "tls-rustls-ring",
    feature = "tls-native-tls"
))]
pub use crate::{TlsConfig, TlsConnector};
