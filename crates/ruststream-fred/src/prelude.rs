//! The imports a service on Redis writes every time, in one glob.
//!
//! `use ruststream_fred::prelude::*;` brings in the broker, the three subscription descriptors,
//! every publish policy, the seek positions, and the extension trait that carries this crate's
//! publish-builder adapters. It re-exports the core prelude first, so one import serves a service
//! file.
//!
//! The core's own prelude stops short of brokers, because which broker a service runs on is the
//! one thing every service states for itself. Importing *this* prelude is that statement: the
//! broker-specificity lives in the crate path, so the core glob rides along rather than
//! contradicting it.
//!
//! It is also this broker's capability manifest, carrying the capability traits Redis implements
//! here *that a service writes*: in a bound ([`TransactionalPublisher`], [`OwnedTransactions`]), or
//! by calling their methods on a value the handler is handed ([`Seeker`], [`Positioned`],
//! [`Partitioned`], and [`Transaction`] on what `OwnedTransactions` returns). So what the glob puts
//! in scope is what this transport can do, and what it omits, it cannot: `RequestReply` would sit
//! in the first group and is absent because Redis has no request-reply primitive - that absence is
//! the manifest working, not an oversight. Traits the runtime consumes on a service's behalf stay
//! out even where implemented; see the note by the exclusions below. A service that globs two
//! broker preludes is safe: the shared items are the same core traits, so the globs unify and the
//! compiler checks it.
//!
//! # Examples
//!
//! ```
//! use ruststream_fred::prelude::*;
//!
//! // The descriptor is pure declaration, so it needs no connection to build.
//! let orders = RedisStream::new("orders").group("workers");
//! let broker = RedisBroker::standalone("redis://localhost:6379");
//! let _ = (orders, broker);
//! ```

// First, so a service file needs one import: the application object, the handler surface, the
// publishing types and the macros all arrive with the broker rather than beside it.
pub use ruststream::prelude::*;

// The capability manifest: the capability traits Redis implements here that a service actually
// writes - either in a bound (`TransactionalPublisher`, `OwnedTransactions`) or by calling their
// methods on a value it is handed (`Seeker::seek`, `Positioned::position`,
// `Partitioned::partition_key`, and `Transaction` on the value `OwnedTransactions` returns).
// `RequestReply` would belong to the first group, and is deliberately not here: Redis has no
// primitive for it.
pub use ruststream::{
    OwnedTransactions, Partitioned, Positioned, Seeker, Transaction, TransactionalPublisher,
};

pub use crate::{
    DelayedRetry, PubSubMode, RedisBroker, RedisGroupPosition, RedisGroupSeeker, RedisList,
    RedisListPublish, RedisPubSub, RedisPubSubPublish, RedisPublish, RedisPublishExt, RedisStream,
    StreamStart,
};

// TLS is configured where the connection is built, and these are `fred`'s own types re-exported
// so a caller need not depend on `fred` directly to name them. They ride the same feature gate as
// the builder methods that take them.
#[cfg(any(
    feature = "tls-rustls",
    feature = "tls-rustls-ring",
    feature = "tls-native-tls"
))]
pub use crate::{TlsConfig, TlsConnector};

// Deliberately absent, so this glob stays the service-author's surface:
//
// - The `testing` module (`RedisTestBroker` and friends): feature-gated harness tooling, imported
//   explicitly by the tests that use it, not by the service under test.
// - `Seekable` and `BatchSubscriber`, both implemented here: they are subscriber-side, and the
//   runtime's plumbing consumes them. A service names the seeker type and declares the batch form
//   in the subscriber attribute, so it never writes either trait.
// - The live and delivered forms - the connected and closed brokers, the subscribers, the
//   publishers, the transaction and message types, `PartitionKeyed`, `EntryId`. A service reaches
//   them through the builder and the handler signature; code that names one is working a layer
//   down and says so by importing it.
// - `RedisError`: a service names errors where it handles them, not everywhere it might.
// - The well-known header constants (`PARTITION_KEY_HEADER` and the dead-letter trio): each is
//   named at the one call site that reads or overrides that header.
