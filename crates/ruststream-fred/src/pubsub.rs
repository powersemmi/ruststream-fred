//! Redis Pub/Sub: delivery to the subscribers connected when a message is published.
//!
//! - [PubSub][pubsub]: [`RedisPubSub`], one channel.
//! - [RedisPubSubPattern][pattern]: [`RedisPubSubPattern`], every channel a glob matches.
//! - [PubSubMode][mode]: [`PubSubMode`], classic or sharded delivery.
//! - [buffer][buffer]: [`RedisPubSub::buffer`], what a busy subscription holds.
//! - [Envelopes][envelopes]: [`RedisPubSub::envelope`], how the headers travel beside the
//!   payload.
//! - [RedisPubSubPublish][publish]: [`Publish`], `PUBLISH` or `SPUBLISH`.
//! - [Prelude][prelude]: [`prelude`], everything a Pub/Sub service imports.
//!
//! [pubsub]: https://powersemmi.github.io/ruststream-fred/latest/components/pubsub/
//! [pattern]: https://powersemmi.github.io/ruststream-fred/latest/components/pubsub/pattern/
//! [mode]: https://powersemmi.github.io/ruststream-fred/latest/components/pubsub/pubsub-mode/
//! [buffer]: https://powersemmi.github.io/ruststream-fred/latest/components/pubsub/buffer/
//! [envelopes]: https://powersemmi.github.io/ruststream-fred/latest/components/subscriptions/envelopes/
//! [publish]: https://powersemmi.github.io/ruststream-fred/latest/components/publishing/pubsub-publish/
//! [prelude]: https://powersemmi.github.io/ruststream-fred/latest/components/broker/prelude/

use std::fmt::{Debug, Formatter};
use std::future::{Future, ready};
use std::num::NonZeroUsize;
use std::sync::Arc;

use bytes::Bytes;
use fred::clients::{Client, Pool};
use fred::interfaces::{ClientLike, PubsubInterface};
use fred::types::config::Server;
use fred::types::{Message, MessageKind, Value};
use futures::Stream;
use futures::stream::unfold;
#[cfg(feature = "asyncapi")]
use ruststream::asyncapi::Bindings;
use ruststream::runtime::IntoSource;
use ruststream::{
    AckError, AddressedCopies, BatchSubscriber, BufferedSubscriber, HeaderMap, IncomingMessage,
    Lend, NamedCopies, OutgoingMessage, PairError, Partitioned, PublishPolicy, Publisher,
    RedeliveryAddress, RedeliveryAddressed, RetryDeclaration, SubscriptionSource,
};
use tokio::runtime::Handle;
use tokio::sync::broadcast::Receiver;
use tokio::sync::broadcast::error::{RecvError, TryRecvError};
use tokio::task::JoinHandle;
use tracing::warn;

use crate::broker::{ConnectedRedisBroker, RedisCore};
use crate::envelope::{Body, Envelope, EnvelopeChoice, Inherited, RustStream};
use crate::loopback::{InFlight, Tap};
use crate::partition::{RedisPublishOptions, resolved_headers};
use crate::pipeline::{PubSubForm, RoundMessage, Window};
use crate::publisher::joins_round;
use crate::{error::RedisError, message::PARTITION_KEY_HEADER};

/// This form's publish policy, [`RedisPubSubPublish`], under the mount-site name every form gives
/// its own. Its options, the delivery mode and the framing codec, still chain off it.
pub use crate::pubsub::RedisPubSubPublish as Publish;

/// The core prelude plus everything a Redis Pub/Sub service writes.
///
/// The broker, the [`RedisPubSub`] descriptor with its [`PubSubMode`], this form's [`Publish`]
/// policy, and its per-delivery context. Pub/Sub carries no core capability traits.
///
/// # Examples
///
/// ```
/// # mod demo {
/// use ruststream_fred::pubsub::prelude::*;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Deserialize)]
/// struct Event {
///     kind: String,
/// }
///
/// #[derive(Serialize, Outgoing)]
/// #[outgoing(name = "events.seen")]
/// struct Seen {
///     kind: String,
/// }
///
/// #[subscriber(RedisPubSub::new("events").mode(PubSubMode::Sharded), reply)]
/// async fn on_event(event: &Event) -> Seen {
///     Seen {
///         kind: event.kind.clone(),
///     }
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("events", "0.1.0")).with_broker(
///         RedisBroker::cluster(["10.0.0.1:6379"]),
///         |b| {
///             // Sharded in, sharded out: an `SPUBLISH` reaches only sharded subscribers.
///             b.include(on_event)
///                 .out_reply(Publish::new().mode(PubSubMode::Sharded));
///         },
///     )
/// }
/// # }
/// # fn main() {}
/// ```
///
/// Two vocabularies that do not mix. A handler body imports `ruststream::prelude::*` and bounds an
/// injected slot with the broker capability trait it needs (`Out<impl Publisher>`); a routes file
/// globs this prelude and names the policy by its mount-site word, the same word on every form.
///
/// The
/// [Prelude](https://powersemmi.github.io/ruststream-fred/latest/components/broker/prelude/)
/// page shows each form's prelude in a service.
///
/// A file that also globs another form's prelude sees an ambiguous `Publish`; use
/// [`crate::prelude`] and write `pubsub::Publish` there.
pub mod prelude {
    pub use ruststream::prelude::*;

    pub use super::{PubSubMode, Publish, RedisPubSub, RedisPubSubPattern};
    // `keys` arrives as the module, not as a glob: its members are short words a service also uses
    // for its own types, and `Ctx<keys::Channel>` reads as what it is at the use site.
    pub use crate::context::{PipelineContext, PoolContext, PubSubContext, keys};
    pub use crate::pipeline::{Bindable, InRound, RedisPipelineSteps};
    pub use crate::{
        FastStream, Framed, NoOp, PARTITION_KEY_HEADER, RedisBroker, RedisEnvelopeExt,
        RedisPublishOptions, RedisPublishSteps, envelope,
    };

    #[cfg(any(
        feature = "tls-rustls",
        feature = "tls-rustls-ring",
        feature = "tls-native-tls"
    ))]
    pub use crate::{TlsConfig, TlsConnector};
}

/// Pub/Sub delivery mode. Defaults to [`Classic`](Self::Classic).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum PubSubMode {
    /// `SUBSCRIBE` / `PUBLISH`: cluster-wide broadcast, pattern-capable, does not scale by slot.
    #[default]
    Classic,
    /// `SSUBSCRIBE` / `SPUBLISH` (Redis 7+): slot-local sharded delivery, no patterns.
    Sharded,
}

impl PubSubMode {
    /// The word the generated `AsyncAPI` document names this mode by.
    #[cfg(feature = "asyncapi")]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Classic => "classic",
            Self::Sharded => "sharded",
        }
    }
}

/// Describes one Pub/Sub subscription on a single channel, against a [`ConnectedRedisBroker`].
///
/// A glob is [`RedisPubSubPattern`] instead.
///
/// # Examples
///
/// ```
/// # mod demo {
/// use ruststream_fred::pubsub::prelude::*;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Event {
///     kind: String,
/// }
///
/// // Classic: `SUBSCRIBE`, broadcast to every node of a cluster.
/// #[subscriber(RedisPubSub::new("events"))]
/// async fn on_event(event: &Event) -> HandlerOutcome {
///     tracing::info!(event.kind, "event");
///     HandlerOutcome::ack()
/// }
///
/// // Sharded: `SSUBSCRIBE`, served by the node that owns the channel's slot.
/// #[subscriber(RedisPubSub::new("metrics").mode(PubSubMode::Sharded))]
/// async fn on_metric(event: &Event) -> HandlerOutcome {
///     tracing::info!(event.kind, "metric");
///     HandlerOutcome::ack()
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("events", "0.1.0")).with_broker(
///         RedisBroker::cluster(["10.0.0.1:6379"]),
///         |b| {
///             b.include(on_event);
///             b.include(on_metric);
///         },
///     )
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Clone)]
#[must_use]
pub struct RedisPubSub<Env = Inherited> {
    channel: String,
    mode: PubSubMode,
    envelope: Env,
    /// Where the mount site sends a spent delivery, taken from its retry declaration when the
    /// subscription opens, so the broker publishes to that name as the channel it is.
    dead_letter: Option<String>,
    /// `None` keeps the capacity `fred` gives a client's broadcast channels by default.
    buffer: Option<NonZeroUsize>,
}

impl<Env: Debug> Debug for RedisPubSub<Env> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisPubSub")
            .field("channel", &self.channel)
            .field("mode", &self.mode)
            .field("envelope", &self.envelope)
            .field("dead_letter", &self.dead_letter)
            .field("buffer", &self.buffer)
            .finish()
    }
}

impl RedisPubSub {
    /// A subscription on the exact `channel`.
    pub fn new(channel: impl Into<String>) -> Self {
        Self {
            channel: channel.into(),
            mode: PubSubMode::default(),
            envelope: Inherited,
            dead_letter: None,
            buffer: None,
        }
    }
}

impl<Env> RedisPubSub<Env> {
    /// Reads through `envelope` instead of the broker's: how a message's headers sit beside its
    /// payload. The publishers that write the same name use the same envelope.
    ///
    /// The manual path names the envelope here. Under `#[subscriber(..)]` the descriptor's type is
    /// fixed by its constructor, so the mount site names it instead, with
    /// [`RedisEnvelopeExt::envelope`](crate::RedisEnvelopeExt::envelope).
    ///
    /// # Examples
    ///
    /// ```
    /// # mod demo {
    /// use std::future::{Future, ready};
    ///
    /// use ruststream::schemars::{self, JsonSchema};
    /// use ruststream_fred::prelude::*;
    /// use serde::Deserialize;
    ///
    /// #[derive(Deserialize, JsonSchema)]
    /// struct Audit {
    ///     id: u64,
    /// }
    ///
    /// /// Logs every audit record.
    /// struct Log;
    ///
    /// impl Handle<Audit> for Log {
    ///     fn handle(
    ///         &self,
    ///         audit: &Audit,
    ///         _outs: &(),
    ///         _ctx: &mut Context<'_>,
    ///     ) -> impl Future<Output = Result<(), HandlerOutcome>> {
    ///         tracing::info!(audit.id, "audit");
    ///         ready(Ok(()))
    ///     }
    /// }
    ///
    /// #[ruststream::app]
    /// fn app() -> impl App {
    ///     RustStream::new(AppInfo::new("audit", "0.1.0")).with_broker(
    ///         RedisBroker::standalone("redis://localhost:6379"),
    ///         |b| {
    ///             // A faststream service writes these.
    ///             let source = RedisPubSub::new("audit").envelope(FastStream);
    ///             b.include(subscriber(source, Log).build());
    ///         },
    ///     )
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    pub fn envelope<Other: Envelope>(self, envelope: Other) -> RedisPubSub<Other> {
        RedisPubSub {
            channel: self.channel,
            mode: self.mode,
            envelope,
            dead_letter: self.dead_letter,
            buffer: self.buffer,
        }
    }

    /// The envelope this descriptor names, resolved against the broker's when it subscribes.
    pub(crate) const fn envelope_choice(&self) -> &Env {
        &self.envelope
    }

    /// Sets the delivery mode. Defaults to [`PubSubMode::Classic`].
    pub const fn mode(mut self, mode: PubSubMode) -> Self {
        self.mode = mode;
        self
    }

    /// Sets how many messages the subscription holds that its handler has not taken yet.
    ///
    /// Redis pushes a Pub/Sub message the moment it is published, and the subscription keeps it
    /// until the handler asks for the next one. The buffer bounds how many may wait. Once it is
    /// full, each new message drops the oldest, and the subscription logs a warning with the
    /// channel and the count it lost. The default is `fred`'s own
    /// (`PerformanceConfig::broadcast_channel_capacity`, 32 messages in `fred` 10.1).
    ///
    /// Raise it for a handler that falls behind bursts: set it to the largest burst the handler
    /// must absorb. The room is reserved when the subscription opens, about 600 bytes per message
    /// of buffer whatever the payloads weigh, because `fred` sizes all eight of a client's
    /// notification channels by it.
    ///
    /// # Examples
    ///
    /// ```
    /// # mod demo {
    /// use ruststream::nonzero;
    /// use ruststream_fred::pubsub::prelude::*;
    /// # #[derive(serde::Deserialize)]
    /// # struct Event { kind: String }
    ///
    /// // A handler that falls behind a burst of up to ten thousand events loses none of them.
    /// #[subscriber(RedisPubSub::new("events").buffer(nonzero!(10_000)))]
    /// async fn on_event(event: &Event) -> HandlerOutcome {
    ///     tracing::info!(event.kind, "event");
    ///     HandlerOutcome::ack()
    /// }
    /// #
    /// # fn app() -> RustStream {
    /// #     RustStream::new(AppInfo::new("events", "0.1.0")).with_broker(
    /// #         RedisBroker::standalone("redis://localhost:6379"),
    /// #         |b| {
    /// #             b.include(on_event);
    /// #         },
    /// #     )
    /// # }
    /// # }
    /// ```
    pub const fn buffer(mut self, messages: NonZeroUsize) -> Self {
        self.buffer = Some(messages);
        self
    }

    /// The channel this subscription listens on.
    #[must_use]
    pub fn channel(&self) -> &str {
        &self.channel
    }

    pub(crate) const fn buffer_size(&self) -> Option<NonZeroUsize> {
        self.buffer
    }

    pub(crate) const fn delivery_mode(&self) -> PubSubMode {
        self.mode
    }

    /// The dead-letter destination the mount site declared, once the registration has handed its
    /// declaration over.
    pub(crate) fn dead_letter(&self) -> Option<&str> {
        self.dead_letter.as_deref()
    }

    /// Takes the dead-letter destination out of the mount site's declaration.
    fn with_declaration(mut self, declaration: &RetryDeclaration) -> Self {
        self.dead_letter = declaration.dead_letter().map(str::to_owned);
        self
    }

    /// Where a publisher reaches this subscription again, shared by both broker forms.
    fn redelivery_channel(&self) -> RedeliveryAddress {
        RedeliveryAddress::new(self.channel.clone())
    }

    /// What this subscription adds to its channel in the generated `AsyncAPI` document, shared by
    /// both broker forms: the delivery mode it subscribes in and how headers are framed.
    #[cfg(feature = "asyncapi")]
    fn describe(&self, envelope: crate::asyncapi::Envelope) -> Bindings {
        crate::asyncapi::channel(&crate::asyncapi::PubSubSubscription::new(
            self.mode.as_str(),
            false,
            envelope,
        ))
    }
}

/// Describes one Pub/Sub subscription on a glob pattern (`PSUBSCRIBE`), against a
/// [`ConnectedRedisBroker`].
///
/// Patterns are classic-only: sharded Pub/Sub routes by slot and has no `PSUBSCRIBE`, so this
/// descriptor carries no mode to get wrong.
///
/// # Examples
///
/// ```
/// # mod demo {
/// use ruststream_fred::pubsub::prelude::*;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Event {
///     kind: String,
/// }
///
/// #[subscriber(RedisPubSubPattern::new("events.*"))]
/// async fn on_any_event(event: &Event, Ctx(channel): Ctx<keys::Channel>) -> HandlerOutcome {
///     tracing::info!(event.kind, channel, "event");
///     HandlerOutcome::ack()
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("events", "0.1.0")).with_broker(
///         RedisBroker::standalone("redis://localhost:6379"),
///         |b| {
///             // A glob is no channel a `PUBLISH` can name, so the mount site says where a
///             // retry copy goes.
///             b.include(on_any_event)
///                 .out_retry(Publish::default())
///                 .to("events.retry");
///         },
///     )
/// }
/// # }
/// # fn main() {}
/// ```
///
/// Sharded Pub/Sub has no pattern subscription, so a pattern takes no mode:
///
/// ```compile_fail
/// use ruststream_fred::{PubSubMode, RedisPubSubPattern};
///
/// let events = RedisPubSubPattern::new("events.*").mode(PubSubMode::Sharded);
/// ```
#[derive(Clone)]
#[must_use]
pub struct RedisPubSubPattern<Env = Inherited> {
    pattern: String,
    envelope: Env,
    /// `None` keeps the capacity `fred` gives a client's broadcast channels by default.
    buffer: Option<NonZeroUsize>,
}

impl<Env: Debug> Debug for RedisPubSubPattern<Env> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisPubSubPattern")
            .field("pattern", &self.pattern)
            .field("envelope", &self.envelope)
            .field("buffer", &self.buffer)
            .finish()
    }
}

impl RedisPubSubPattern {
    /// A subscription on every channel matching the glob `pattern`.
    pub fn new(pattern: impl Into<String>) -> Self {
        Self {
            pattern: pattern.into(),
            envelope: Inherited,
            buffer: None,
        }
    }
}

impl<Env> RedisPubSubPattern<Env> {
    /// Reads through `envelope` instead of the broker's: how a message's headers sit beside its
    /// payload. The publishers that write the same name use the same envelope.
    ///
    /// The manual path names the envelope here. Under `#[subscriber(..)]` the descriptor's type is
    /// fixed by its constructor, so the mount site names it instead, with
    /// [`RedisEnvelopeExt::envelope`](crate::RedisEnvelopeExt::envelope).
    ///
    /// # Examples
    ///
    /// ```
    /// # mod demo {
    /// use std::future::{Future, ready};
    ///
    /// use ruststream::schemars::{self, JsonSchema};
    /// use ruststream_fred::prelude::*;
    /// use serde::Deserialize;
    ///
    /// #[derive(Deserialize, JsonSchema)]
    /// struct Audit {
    ///     id: u64,
    /// }
    ///
    /// /// Logs every audit record.
    /// struct Log;
    ///
    /// impl Handle<Audit> for Log {
    ///     fn handle(
    ///         &self,
    ///         audit: &Audit,
    ///         _outs: &(),
    ///         _ctx: &mut Context<'_>,
    ///     ) -> impl Future<Output = Result<(), HandlerOutcome>> {
    ///         tracing::info!(audit.id, "audit");
    ///         ready(Ok(()))
    ///     }
    /// }
    ///
    /// #[ruststream::app]
    /// fn app() -> impl App {
    ///     RustStream::new(AppInfo::new("audit", "0.1.0")).with_broker(
    ///         RedisBroker::standalone("redis://localhost:6379"),
    ///         |b| {
    ///             // A faststream service writes these.
    ///             let source = RedisPubSubPattern::new("audit.*").envelope(FastStream);
    ///             b.include(subscriber(source, Log).build());
    ///         },
    ///     )
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    pub fn envelope<Other: Envelope>(self, envelope: Other) -> RedisPubSubPattern<Other> {
        RedisPubSubPattern {
            pattern: self.pattern,
            envelope,
            buffer: self.buffer,
        }
    }

    /// The envelope this descriptor names, resolved against the broker's when it subscribes.
    pub(crate) const fn envelope_choice(&self) -> &Env {
        &self.envelope
    }

    /// Sets how many messages the subscription holds that its handler has not taken yet.
    ///
    /// Redis pushes a Pub/Sub message the moment it is published, and the subscription keeps it
    /// until the handler asks for the next one. The buffer bounds how many may wait. Once it is
    /// full, each new message drops the oldest, and the subscription logs a warning with the
    /// channel and the count it lost. The default is `fred`'s own
    /// (`PerformanceConfig::broadcast_channel_capacity`, 32 messages in `fred` 10.1).
    ///
    /// Raise it for a handler that falls behind bursts: set it to the largest burst the handler
    /// must absorb. The room is reserved when the subscription opens, about 600 bytes per message
    /// of buffer whatever the payloads weigh, because `fred` sizes all eight of a client's
    /// notification channels by it.
    ///
    /// # Examples
    ///
    /// ```
    /// # mod demo {
    /// use ruststream::nonzero;
    /// use ruststream_fred::pubsub::prelude::*;
    /// # #[derive(serde::Deserialize)]
    /// # struct Event { kind: String }
    ///
    /// // A handler that falls behind a burst of up to ten thousand events loses none of them.
    /// #[subscriber(RedisPubSubPattern::new("events.*").buffer(nonzero!(10_000)))]
    /// async fn on_event(event: &Event) -> HandlerOutcome {
    ///     tracing::info!(event.kind, "event");
    ///     HandlerOutcome::ack()
    /// }
    /// #
    /// # fn app() -> RustStream {
    /// #     RustStream::new(AppInfo::new("events", "0.1.0")).with_broker(
    /// #         RedisBroker::standalone("redis://localhost:6379"),
    /// #         |b| {
    /// #             b.include(on_event);
    /// #         },
    /// #     )
    /// # }
    /// # }
    /// ```
    pub const fn buffer(mut self, messages: NonZeroUsize) -> Self {
        self.buffer = Some(messages);
        self
    }

    /// The glob this subscription matches channels against.
    #[must_use]
    pub fn pattern(&self) -> &str {
        &self.pattern
    }

    pub(crate) const fn buffer_size(&self) -> Option<NonZeroUsize> {
        self.buffer
    }

    /// Patterns are classic-only, so the mode in the document is the only one they have, and the
    /// envelope is all a pattern adds to it.
    #[cfg(feature = "asyncapi")]
    fn describe(envelope: crate::asyncapi::Envelope) -> Bindings {
        crate::asyncapi::channel(&crate::asyncapi::PubSubSubscription::new(
            PubSubMode::Classic.as_str(),
            true,
            envelope,
        ))
    }
}

// The manual path binds a `Handle` body to its source with `subscriber(source, body)`, which
// takes the descriptor as it is, the way the `#[subscriber(..)]` attribute does.
impl<Env> IntoSource for RedisPubSub<Env> {
    type Source = Self;

    fn into_source(self) -> Self {
        self
    }
}

impl<BrokerEnv, Env> SubscriptionSource<ConnectedRedisBroker<BrokerEnv>> for RedisPubSub<Env>
where
    BrokerEnv: Envelope,
    Env: EnvelopeChoice<BrokerEnv>,
{
    type Subscriber = RedisPubSubSubscriber<Env::Envelope>;
    /// Pub/Sub cannot acknowledge, so every retry is a copy this process publishes, and the
    /// channel is where it goes.
    type Copies = AddressedCopies;

    fn name(&self) -> &str {
        self.channel()
    }

    fn declare_retry(self, declaration: &RetryDeclaration) -> Self {
        self.with_declaration(declaration)
    }

    async fn subscribe(
        self,
        connected: &ConnectedRedisBroker<BrokerEnv>,
    ) -> Result<Self::Subscriber, RedisError> {
        connected.subscribe_pubsub(self).await
    }

    #[cfg(feature = "asyncapi")]
    fn channel_bindings(&self) -> Bindings {
        self.describe(crate::asyncapi::Envelope::of::<Env::Envelope>())
    }
}

/// The channel itself.
///
/// The answer is about the address, not about the command: reaching a
/// [`PubSubMode::Sharded`] subscription still takes a publisher in the same mode, which is the
/// policy the mount site names with `out_retry`.
impl<BrokerEnv, Env> RedeliveryAddressed<ConnectedRedisBroker<BrokerEnv>> for RedisPubSub<Env>
where
    BrokerEnv: Envelope,
    Env: EnvelopeChoice<BrokerEnv>,
{
    fn redelivery_address(
        &self,
        _connected: &ConnectedRedisBroker<BrokerEnv>,
    ) -> impl Future<Output = Result<RedeliveryAddress, RedisError>> {
        ready(Ok(self.redelivery_channel()))
    }
}

// The manual path binds a `Handle` body to its source with `subscriber(source, body)`, which
// takes the descriptor as it is, the way the `#[subscriber(..)]` attribute does.
impl<Env> IntoSource for RedisPubSubPattern<Env> {
    type Source = Self;

    fn into_source(self) -> Self {
        self
    }
}

/// A pattern subscription reads every channel the glob matches, and a glob is not a channel a
/// `PUBLISH` can name, so the mount site names where a retry copy goes: `.out_retry(policy).to(..)`
/// for a fixed channel, or a transform that reads the delivery's own channel.
impl<BrokerEnv, Env> SubscriptionSource<ConnectedRedisBroker<BrokerEnv>> for RedisPubSubPattern<Env>
where
    BrokerEnv: Envelope,
    Env: EnvelopeChoice<BrokerEnv>,
{
    type Subscriber = RedisPubSubSubscriber<Env::Envelope>;
    type Copies = NamedCopies;

    fn name(&self) -> &str {
        self.pattern()
    }

    async fn subscribe(
        self,
        connected: &ConnectedRedisBroker<BrokerEnv>,
    ) -> Result<Self::Subscriber, RedisError> {
        connected.subscribe_pubsub_pattern(self).await
    }

    #[cfg(feature = "asyncapi")]
    fn channel_bindings(&self) -> Bindings {
        Self::describe(crate::asyncapi::Envelope::of::<Env::Envelope>())
    }
}

/// A Pub/Sub subscription backed by a dedicated `fred` client, so its message stream and channel
/// state are isolated from other subscribers and from the publishing pool.
///
/// Pub/Sub delivers one message at a time, so the batches a [`BatchSubscriber`] hands out are
/// assembled on the client by the core's [`BufferedSubscriber`]: it fills a batch up to the size
/// the mount site asked for and closes a partial one on its own deadline.
pub struct RedisPubSubSubscriber<Env = RustStream>(BufferedSubscriber<PubSubWire<Env>>);

impl<Env> Debug for RedisPubSubSubscriber<Env> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisPubSubSubscriber")
            .finish_non_exhaustive()
    }
}

impl<Env: Envelope> RedisPubSubSubscriber<Env> {
    pub(crate) fn new(wire: PubSubWire<Env>) -> Self {
        Self(BufferedSubscriber::new(wire))
    }
}

impl<Env: Envelope> ruststream::Subscriber for RedisPubSubSubscriber<Env> {
    type Message = RedisPubSubMessage<Env>;
    type Error = RedisError;

    /// Yields one message per Pub/Sub delivery.
    ///
    /// # Cancel safety
    ///
    /// Dropping the returned stream between items is safe. What arrives while no stream is polling
    /// waits in the subscription's [buffer](crate::RedisPubSub::buffer); past it the oldest is
    /// lost, and while the subscription is disconnected everything is, as Redis Pub/Sub keeps
    /// nothing for a subscriber that is not there.
    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        self.0.stream()
    }
}

impl<Env: Envelope> BatchSubscriber for RedisPubSubSubscriber<Env> {
    type Batch = Vec<RedisPubSubMessage<Env>>;

    /// Yields batches of at most `size` deliveries, assembled as they arrive.
    ///
    /// # Cancel safety
    ///
    /// Same as [`Subscriber::stream`](ruststream::Subscriber::stream). A batch abandoned mid-fill
    /// loses the deliveries it holds, as any Pub/Sub delivery nobody is polling for is lost.
    fn batches(
        &mut self,
        size: NonZeroUsize,
    ) -> impl Stream<Item = Result<Self::Batch, Self::Error>> + Send + '_ {
        self.0.batches(size)
    }
}

/// What a Pub/Sub subscription's client subscribes to, kept so it can subscribe again.
pub(crate) enum Resubscribe {
    Channel(String),
    ShardedChannel(String),
    Pattern(String),
}

/// The dedicated client of a Pub/Sub subscription, and the task that subscribes it again each
/// time it reconnects.
///
/// The server forgets a connection's subscriptions when the connection drops (a restart, a
/// failover, a killed client), and `fred` reconnects the socket but does not repeat the
/// `SUBSCRIBE`. Messages published between the drop and the new subscribe reach nobody, which is
/// what Pub/Sub promises; every later one arrives. The task costs one spawn per subscription and
/// nothing per message.
pub(crate) struct Resubscribing {
    client: Client,
    resubscribing: JoinHandle<()>,
}

impl Resubscribing {
    /// Starts the task on `runtime`. `reconnects` is taken from `client` before its first
    /// subscribe, so a reconnect that lands in between is not missed.
    pub(crate) fn new(
        client: Client,
        mut reconnects: Receiver<Server>,
        target: Resubscribe,
        runtime: &Handle,
    ) -> Self {
        let resubscriber = client.clone();
        let resubscribing = runtime.spawn(async move {
            loop {
                match reconnects.recv().await {
                    Ok(_) | Err(RecvError::Lagged(_)) => {}
                    Err(RecvError::Closed) => return,
                }
                let (name, outcome) = match &target {
                    Resubscribe::Channel(name) => (name, resubscriber.subscribe(name).await),
                    Resubscribe::ShardedChannel(name) => {
                        (name, resubscriber.ssubscribe(name).await)
                    }
                    Resubscribe::Pattern(name) => (name, resubscriber.psubscribe(name).await),
                };
                if let Err(error) = outcome {
                    warn!(
                        subscription = name.as_str(),
                        %error,
                        "a Redis Pub/Sub subscription reconnected but could not subscribe again; \
                         it receives nothing until its connection drops and reconnects"
                    );
                }
            }
        });
        Self {
            client,
            resubscribing,
        }
    }
}

/// The wire side of a Pub/Sub subscription: the dedicated client and the channel it feeds.
pub(crate) struct PubSubWire<Env> {
    /// The channel or the glob it reads, for the warning a lost message raises.
    name: String,
    client: Resubscribing,
    rx: Receiver<Message>,
    envelope: Env,
    /// The broker's pool, handed to every delivery for `Ctx<keys::FredPool>`.
    pool: Pool,
    /// The subscription's registration with the in-process server; empty on a real connection.
    tap: Tap,
    /// The broker's runtime, where the dedicated client is closed once the subscription stops.
    runtime: Handle,
}

impl<Env> Debug for PubSubWire<Env> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PubSubWire").finish_non_exhaustive()
    }
}

impl<Env: Envelope> PubSubWire<Env> {
    pub(crate) fn new(
        name: String,
        client: Resubscribing,
        rx: Receiver<Message>,
        envelope: Env,
        pool: Pool,
        tap: Tap,
        runtime: Handle,
    ) -> Self {
        Self {
            name,
            client,
            rx,
            envelope,
            pool,
            tap,
            runtime,
        }
    }

    /// The window's client: a connection of the pool the flushes go out on.
    pub(crate) fn round_client(&self) -> Client {
        self.pool.next().clone()
    }

    /// Yields one delivery per message, each with a slot in `window`, telling the window how
    /// many more have arrived behind it, and reports a failed flush of the window once.
    pub(crate) fn round_stream<'a>(
        &'a mut self,
        window: &'a Arc<Window<PubSubForm<Env>>>,
    ) -> impl Stream<Item = Result<RoundMessage<PubSubForm<Env>>, RedisError>> + Send + 'a {
        let (envelope, pool, tap, name) =
            (&self.envelope, &self.pool, &self.tap, self.name.as_str());
        unfold(&mut self.rx, move |rx| async move {
            loop {
                if let Some(failure) = window.take_failure() {
                    return Some((Err(failure), rx));
                }
                match rx.recv().await {
                    Ok(msg) => {
                        let delivery = to_message(msg, envelope, pool, tap);
                        window.yielded(rx.len());
                        let round = window.open();
                        return Some((
                            Ok(RoundMessage::new(delivery, Arc::clone(window), round)),
                            rx,
                        ));
                    }
                    // The receiver fell behind the broadcast buffer; skip the gap and keep reading.
                    Err(RecvError::Lagged(skipped)) => lagged(name, tap, skipped),
                    Err(RecvError::Closed) => return None,
                }
            }
        })
    }
}

/// Reports the messages a subscription that fell behind its buffer lost, and forgets them.
fn lagged(name: &str, tap: &Tap, skipped: u64) {
    warn!(
        subscription = name,
        lost = skipped,
        "a Redis Pub/Sub subscription fell behind its buffer and lost the oldest messages; raise \
         its buffer or speed up its handler"
    );
    #[cfg(feature = "testing")]
    tap.discard(usize::try_from(skipped).unwrap_or(usize::MAX));
    #[cfg(not(feature = "testing"))]
    let _ = tap;
}

impl<Env: Envelope> PubSubWire<Env> {
    /// Yields one batch of up to `size` messages: the first one waited for, and those already
    /// arrived behind it. The batch shares one slot of `window`.
    pub(crate) fn round_batches<'a>(
        &'a mut self,
        window: &'a Arc<Window<PubSubForm<Env>>>,
        size: NonZeroUsize,
    ) -> impl Stream<Item = Result<Vec<RoundMessage<PubSubForm<Env>>>, RedisError>> + Send + 'a
    {
        window.fill_at(size.get());
        let (envelope, pool, tap, name) =
            (&self.envelope, &self.pool, &self.tap, self.name.as_str());
        unfold(&mut self.rx, move |rx| async move {
            loop {
                if let Some(failure) = window.take_failure() {
                    return Some((Err(failure), rx));
                }
                let first = match rx.recv().await {
                    Ok(msg) => msg,
                    Err(RecvError::Lagged(skipped)) => {
                        lagged(name, tap, skipped);
                        continue;
                    }
                    Err(RecvError::Closed) => return None,
                };
                let mut messages = vec![first];
                while messages.len() < size.get() {
                    match rx.try_recv() {
                        Ok(msg) => messages.push(msg),
                        Err(TryRecvError::Lagged(skipped)) => lagged(name, tap, skipped),
                        Err(_) => break,
                    }
                }
                let members = u32::try_from(messages.len()).unwrap_or(u32::MAX);
                for _ in 0..members {
                    window.yielded(rx.len());
                }
                let round = window.open_batch(members);
                let batch = messages
                    .into_iter()
                    .map(|msg| {
                        RoundMessage::new(
                            to_message(msg, envelope, pool, tap),
                            Arc::clone(window),
                            round.clone(),
                        )
                    })
                    .collect();
                return Some((Ok(batch), rx));
            }
        })
    }
}

impl<Env> Drop for PubSubWire<Env> {
    fn drop(&mut self) {
        // What reached the subscription and was never read leaves with it: the in-process server
        // counted each message in flight when it sent it.
        #[cfg(feature = "testing")]
        {
            let mut unread = 0_usize;
            loop {
                match self.rx.try_recv() {
                    Ok(_) => unread += 1,
                    Err(TryRecvError::Lagged(skipped)) => {
                        unread =
                            unread.saturating_add(usize::try_from(skipped).unwrap_or(usize::MAX));
                    }
                    Err(_) => break,
                }
            }
            self.tap.discard(unread);
        }
        // The dedicated client owns a background connection task; close it on a detached task since
        // `drop` cannot await, on the broker's runtime, which is there wherever the drop happens.
        self.client.resubscribing.abort();
        let client = self.client.client.clone();
        self.runtime.spawn(async move {
            let _ = client.quit().await;
        });
    }
}

fn to_message<Env: Envelope>(
    msg: Message,
    envelope: &Env,
    pool: &Pool,
    tap: &Tap,
) -> RedisPubSubMessage<Env> {
    // The value is taken over as the client holds it, without a copy.
    let raw = match msg.value {
        Value::Bytes(bytes) => bytes,
        Value::String(text) => text.into_inner(),
        _ => Bytes::new(),
    };
    RedisPubSubMessage {
        channel: msg.channel.to_string(),
        // `PMessage` is the delivery kind for a `PSUBSCRIBE` match; the message's own channel is the
        // concrete one matched, which differs from the subscription's glob pattern.
        pattern: matches!(msg.kind, MessageKind::PMessage),
        body: Body::open(envelope.clone(), raw),
        pool: pool.clone(),
        flight: tap.delivered(),
    }
}

impl<Env: Envelope> ruststream::Subscriber for PubSubWire<Env> {
    type Message = RedisPubSubMessage<Env>;
    type Error = RedisError;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        let (envelope, pool, tap, name) =
            (&self.envelope, &self.pool, &self.tap, self.name.as_str());
        unfold(&mut self.rx, move |rx| async move {
            loop {
                match rx.recv().await {
                    Ok(msg) => {
                        let message = to_message(msg, envelope, pool, tap);
                        return Some((Ok(message), rx));
                    }
                    // The receiver fell behind the broadcast buffer; skip the gap and keep reading.
                    Err(RecvError::Lagged(skipped)) => lagged(name, tap, skipped),
                    Err(RecvError::Closed) => return None,
                }
            }
        })
    }
}

/// A Pub/Sub delivery. `ack` / `nack` are unsupported (Pub/Sub has no acknowledgement).
pub struct RedisPubSubMessage<Env = RustStream> {
    channel: String,
    /// Whether this delivery arrived through a `PSUBSCRIBE` pattern match (vs an exact subscribe).
    pattern: bool,
    /// The message as it arrived: its payload located, its headers read when first asked for.
    body: Body<Env>,
    /// The broker's pool, which `Ctx<keys::FredPool>` hands the handler.
    pool: Pool,
    /// The test harness's count of this delivery; empty outside the in-process mode.
    #[cfg_attr(
        not(feature = "testing"),
        allow(
            dead_code,
            reason = "held for its release on drop, which only `testing` gives"
        )
    )]
    flight: InFlight,
}

impl<Env: Envelope> Debug for RedisPubSubMessage<Env> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisPubSubMessage")
            .field("channel", &self.channel)
            .field("pattern", &self.pattern)
            .field("payload_len", &self.body.len())
            .finish_non_exhaustive()
    }
}

impl<Env> RedisPubSubMessage<Env> {
    /// The channel this message arrived on.
    ///
    /// On a [`RedisPubSubPattern`] subscription this is the concrete channel the message was
    /// published to, which differs from the glob the subscription registered.
    #[must_use]
    pub fn channel(&self) -> &str {
        &self.channel
    }

    /// Whether this delivery arrived through a `PSUBSCRIBE` pattern match rather than an exact
    /// channel subscribe.
    #[must_use]
    pub fn from_pattern(&self) -> bool {
        self.pattern
    }

    /// The broker's connection pool, for the per-delivery context.
    pub(crate) const fn pool(&self) -> &Pool {
        &self.pool
    }

    /// Takes the harness's count of this delivery, for a window to hold until its flush.
    #[cfg(feature = "testing")]
    pub(crate) fn take_flight(&mut self) -> InFlight {
        std::mem::take(&mut self.flight)
    }
}

impl<Env: Envelope> IncomingMessage for RedisPubSubMessage<Env> {
    fn payload(&self) -> &[u8] {
        self.body.payload()
    }

    fn headers(&self) -> &HeaderMap {
        self.body.headers()
    }

    // See `RedisMessage`: the keyed worker lanes read the key from here, not from `Partitioned`.
    fn partition_key(&self) -> Option<&[u8]> {
        Partitioned::partition_key(self)
    }

    fn ack(self) -> impl Future<Output = Result<(), AckError>> {
        ready(Err(AckError::Unsupported))
    }

    fn nack(self, _requeue: bool) -> impl Future<Output = Result<(), AckError>> {
        ready(Err(AckError::Unsupported))
    }
}

impl<Env: Envelope> Partitioned for RedisPubSubMessage<Env> {
    fn partition_key(&self) -> Option<&[u8]> {
        self.headers().get(PARTITION_KEY_HEADER)
    }
}

/// The declaration half of the Pub/Sub publisher: delivery mode and envelope, no connection.
///
/// Constructible anywhere (in a router definition, in configuration), it pairs into a
/// [`RedisPubSubPublisher`] against a [`ConnectedRedisBroker`]. The publish mode must match how
/// subscribers subscribed: a sharded publish only reaches sharded subscribers.
///
/// # Examples
///
/// ```
/// # mod demo {
/// use ruststream_fred::stream::prelude::*;
/// use ruststream_fred::{PubSubMode, RedisPubSubPublish};
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Deserialize)]
/// struct Order {
///     id: u64,
/// }
///
/// #[derive(Serialize, Outgoing)]
/// #[outgoing(name = "orders.live")]
/// struct Placed {
///     id: u64,
/// }
///
/// #[subscriber(RedisStream::new("orders").group("workers"), reply)]
/// async fn announce(order: &Order) -> Placed {
///     Placed { id: order.id }
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
///         RedisBroker::cluster(["10.0.0.1:6379"]),
///         |b| {
///             // Read from a durable stream, broadcast to whoever is listening with `SPUBLISH`.
///             b.include(announce)
///                 .out_reply(RedisPubSubPublish::new().mode(PubSubMode::Sharded));
///         },
///     )
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, Clone)]
#[must_use]
pub struct RedisPubSubPublish<Env = Inherited> {
    mode: PubSubMode,
    envelope: Env,
}

impl Default for RedisPubSubPublish {
    fn default() -> Self {
        Self {
            mode: PubSubMode::default(),
            envelope: Inherited,
        }
    }
}

impl RedisPubSubPublish {
    /// A classic-mode policy in the broker's envelope. Equivalent to [`Self::default`].
    pub fn new() -> Self {
        Self::default()
    }
}

impl<Env> RedisPubSubPublish<Env> {
    /// Writes the messages through `envelope` instead of the broker's: how a message's headers sit
    /// beside its payload. The subscriptions on the channel read the same envelope.
    ///
    /// # Examples
    ///
    /// ```
    /// # mod demo {
    /// use ruststream_fred::pubsub::prelude::*;
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(Deserialize)]
    /// struct Order {
    ///     id: u64,
    /// }
    ///
    /// #[derive(Serialize, Outgoing)]
    /// #[outgoing(name = "orders.live")]
    /// struct Placed {
    ///     id: u64,
    /// }
    ///
    /// #[subscriber(RedisPubSub::new("orders"), reply)]
    /// async fn announce(order: &Order) -> Placed {
    ///     Placed { id: order.id }
    /// }
    ///
    /// #[ruststream::app]
    /// fn app() -> impl App {
    ///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
    ///         RedisBroker::standalone("redis://localhost:6379"),
    ///         |b| {
    ///             // A dashboard that knows nothing of headers listens on `orders.live`.
    ///             b.include(announce).out_reply(Publish::new().envelope(NoOp));
    ///         },
    ///     )
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    pub fn envelope<Other: Envelope>(self, envelope: Other) -> RedisPubSubPublish<Other> {
        RedisPubSubPublish {
            mode: self.mode,
            envelope,
        }
    }

    /// Sets the publish mode. Defaults to [`PubSubMode::Classic`].
    pub const fn mode(mut self, mode: PubSubMode) -> Self {
        self.mode = mode;
        self
    }
}

impl<BrokerEnv, Env> PublishPolicy<ConnectedRedisBroker<BrokerEnv>> for RedisPubSubPublish<Env>
where
    BrokerEnv: Envelope,
    Env: EnvelopeChoice<BrokerEnv>,
{
    type Live = RedisPubSubPublisher<Env::Envelope>;

    fn pair(
        self,
        connected: &ConnectedRedisBroker<BrokerEnv>,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(connected.pubsub_publisher(self)))
    }

    /// The channel a `PUBLISH` goes out on, the delivery mode it goes out in, and how headers are
    /// framed beside the payload.
    #[cfg(feature = "asyncapi")]
    fn channel_bindings(&self, channel: &str) -> Bindings {
        crate::asyncapi::channel(&crate::asyncapi::Publish::pubsub(
            channel,
            self.mode.as_str(),
            crate::asyncapi::Envelope::of::<Env::Envelope>(),
        ))
    }
}

/// Publishes Pub/Sub messages with `PUBLISH` (classic) or `SPUBLISH` (sharded): a
/// [`RedisPubSubPublish`] policy paired with a connection.
///
/// Obtain it from
/// [`ConnectedRedisBroker::pubsub_publisher`](crate::ConnectedRedisBroker::pubsub_publisher), or
/// by pairing the policy. Like every publisher here it may outlive the connection, so publishing
/// after shutdown reports [`RedisError::ShutDown`].
#[derive(Clone)]
pub struct RedisPubSubPublisher<Env = RustStream> {
    core: Arc<RedisCore>,
    mode: PubSubMode,
    envelope: Env,
    /// What `pipeline.bind(&out)` names this publisher and its clones by.
    round_name: u64,
}

impl<Env: Debug> Debug for RedisPubSubPublisher<Env> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisPubSubPublisher")
            .field("mode", &self.mode)
            .field("envelope", &self.envelope)
            .finish_non_exhaustive()
    }
}

impl<Env: Envelope> RedisPubSubPublisher<Env> {
    /// What `pipeline.bind(&out)` names this publisher and its clones by.
    pub(crate) const fn round_name(&self) -> u64 {
        self.round_name
    }

    pub(crate) fn new<BrokerEnv: Envelope, Choice: EnvelopeChoice<BrokerEnv, Envelope = Env>>(
        core: Arc<RedisCore>,
        publish: RedisPubSubPublish<Choice>,
        broker: &BrokerEnv,
    ) -> Self {
        Self {
            round_name: core.rounds().publisher(),
            core,
            mode: publish.mode,
            envelope: publish.envelope.resolve(broker),
        }
    }
}

impl<Env: Envelope> Publisher for RedisPubSubPublisher<Env> {
    /// The payload is read into an envelope of this crate's own before it reaches the client, so
    /// there is nothing to keep: the publish is lent the bytes where they already are.
    type Payload = Lend;

    type Error = RedisError;
    /// `PUBLISH` and `SPUBLISH` take a channel and a payload and nothing else, and which of the
    /// two is issued is the publisher's mode, fixed by the policy, because a sharded publish only
    /// reaches sharded subscribers. What a call site still says is the partition key, framed into
    /// the payload with the message's other headers.
    type Options = RedisPublishOptions;

    async fn publish(
        &self,
        msg: OutgoingMessage<'_>,
        options: Option<&Self::Options>,
    ) -> Result<(), Self::Error> {
        let (channel, payload, headers) = msg.into_parts();
        let mut body = Vec::with_capacity(payload.len());
        self.envelope
            .seal(payload, &resolved_headers(headers, options), &mut body)
            .map_err(|err| RedisError::seal(channel, err))?;
        if let Some(round) = self
            .core
            .rounds()
            .joined(self.round_name, joins_round(options))
        {
            return round
                .publish(channel, body, self.mode == PubSubMode::Sharded)
                .await
                .map_err(RedisError::publish);
        }
        send(&self.core, self.mode, channel, body).await
    }
}

/// Publishes one sealed message on `channel` in `mode`: the Pub/Sub publish, shared by
/// [`RedisPubSubPublisher`] and the broker's default publisher.
pub(crate) async fn send(
    core: &RedisCore,
    mode: PubSubMode,
    channel: &str,
    body: Vec<u8>,
) -> Result<(), RedisError> {
    let pool = core.pool()?;
    let client = pool.next();
    let channel = channel.to_owned();
    let _: i64 = match mode {
        PubSubMode::Classic => client.publish(channel, body).await,
        PubSubMode::Sharded => client.spublish(channel, body).await,
    }
    .map_err(RedisError::publish)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::PubSubContext;
    use fred::types::config::Config;
    use ruststream::BuildContext;

    /// An unconnected pool (just client structs); `Pool::new` opens no sockets.
    fn offline_pool() -> Pool {
        Pool::new(Config::default(), None, None, None, 1).expect("offline pool")
    }

    #[test]
    fn build_context_reads_channel_and_pattern_flag() {
        let exact = RedisPubSubMessage {
            channel: "events".to_owned(),
            pattern: false,
            body: Body::open(RustStream, Bytes::from_static(b"{}")),
            pool: offline_pool(),
            flight: InFlight::default(),
        };
        let cx = PubSubContext::build(&exact);
        assert_eq!(cx.channel(), "events");
        assert!(!cx.from_pattern());

        let matched = RedisPubSubMessage {
            channel: "events.user".to_owned(),
            pattern: true,
            body: Body::open(RustStream, Bytes::from_static(b"{}")),
            pool: offline_pool(),
            flight: InFlight::default(),
        };
        assert!(PubSubContext::build(&matched).from_pattern());
    }

    /// A channel is one destination a `PUBLISH` names, whatever the delivery mode.
    #[test]
    fn a_channel_addresses_its_copies_to_itself() {
        assert_eq!(
            RedisPubSub::new("events").redelivery_channel().as_str(),
            "events"
        );
        assert_eq!(
            RedisPubSub::new("events")
                .mode(PubSubMode::Sharded)
                .redelivery_channel()
                .as_str(),
            "events"
        );
    }
}
