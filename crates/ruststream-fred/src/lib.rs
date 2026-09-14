#![doc = include_str!("README.md")]
#![forbid(unsafe_code)]

#[cfg(feature = "asyncapi")]
mod asyncapi;
mod broker;
mod claim;
mod convert;
mod delay;
mod envelope;
mod error;
mod message;
mod partition;
mod publisher;
mod recovery;
mod seek;
mod settings;
mod subscriber;

// These modules carry their own `//!` summaries. A second doc fragment here would be written in
// the crate root's scope, and rustdoc would then resolve the module's intra-doc links there too.
pub mod context;
pub mod prelude;

// The three transport forms. Each is public for its own `prelude` and its `Publish` alias, so a
// mount site names the policy by the same word whichever form it is on; the types they hold stay
// re-exported at the crate root as well, for a file that mixes forms.
pub mod list;
pub mod pubsub;
pub mod stream;

pub use broker::{ClosedRedisBroker, ConnectedRedisBroker, RedisBroker};
pub use delay::DelayedRetry;
pub use error::RedisError;
pub use list::{
    RedisList, RedisListMessage, RedisListPublish, RedisListPublisher, RedisListSubscriber,
};
pub use message::{DELIVERY_COUNT_HEADER, IDLE_MS_HEADER, PARTITION_KEY_HEADER, RedisMessage};
pub use partition::{RedisPublishOptions, RedisPublishSteps};
pub use publisher::{RedisPublish, RedisPublisher, RedisTransaction};
pub use pubsub::{
    PubSubMode, RedisPubSub, RedisPubSubMessage, RedisPubSubPattern, RedisPubSubPublish,
    RedisPubSubPublisher, RedisPubSubSubscriber,
};
pub use seek::{EntryId, RedisGroupPosition, RedisGroupSeeker};
pub use settings::RedisSubscribeExt;
pub use stream::{RedisStream, StreamStart};
pub use subscriber::RedisSubscriber;

// fred auth/TLS types re-exported for the `RedisBroker::tls` / `::credential_provider` builders, so
// callers need not depend on `fred` directly to name them.
#[cfg(feature = "credential-provider")]
pub use fred::types::config::CredentialProvider;
#[cfg(any(
    feature = "tls-rustls",
    feature = "tls-rustls-ring",
    feature = "tls-native-tls"
))]
pub use fred::types::config::{TlsConfig, TlsConnector};

#[cfg(feature = "testing")]
pub mod testing;
