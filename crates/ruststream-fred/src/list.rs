//! Redis lists: a work queue whose every entry goes to one consumer.
//!
//! - [Lists][lists]: [`RedisList`], the simple form, at-most-once.
//! - [reliable][reliable]: [`RedisList::reliable`], at-least-once through a processing list.
//! - [Recovery][recovery]: [`RedisList::recovery_zset`], the watchdog that returns a dead
//!   consumer's entries.
//! - [processing][processing]: [`RedisList::processing`], and the hash tag a cluster needs.
//! - [Envelopes][envelopes]: [`RedisList::envelope`], how the headers travel beside the payload.
//! - [RedisListPublish][publish]: [`Publish`], `LPUSH` with an optional key expiry.
//! - [Prelude][prelude]: [`prelude`], everything a list service imports.
//!
//! [lists]: https://powersemmi.github.io/ruststream-fred/latest/components/lists/
//! [reliable]: https://powersemmi.github.io/ruststream-fred/latest/components/lists/reliable/
//! [recovery]: https://powersemmi.github.io/ruststream-fred/latest/components/lists/recovery/
//! [processing]: https://powersemmi.github.io/ruststream-fred/latest/components/lists/processing/
//! [envelopes]: https://powersemmi.github.io/ruststream-fred/latest/components/subscriptions/envelopes/
//! [publish]: https://powersemmi.github.io/ruststream-fred/latest/components/publishing/list-publish/
//! [prelude]: https://powersemmi.github.io/ruststream-fred/latest/components/broker/prelude/

use std::collections::VecDeque;
use std::fmt::{Debug, Formatter};
use std::future::{Future, ready};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use fred::clients::{Client, Pool};
use fred::error::ErrorKind;
use fred::interfaces::{KeysInterface, ListInterface, SortedSetsInterface};
use fred::types::Value;
use fred::types::lists::LMoveDirection;
use futures::Stream;
use futures::stream::unfold;
#[cfg(feature = "asyncapi")]
use ruststream::asyncapi::Bindings;
use ruststream::runtime::IntoSource;
use ruststream::{
    AckError, AddressedCopies, BatchSubscriber, BufferedSubscriber, HeaderMap, IncomingMessage,
    Lend, PairError, Partitioned, PublishPolicy, RedeliveryAddress, RedeliveryAddressed,
    RetryDeclaration, SubscriptionSource,
};

use crate::broker::{ConnectedRedisBroker, RedisCore};
use crate::connection::{Connection, ReadConnection};
use crate::envelope::{Body, Envelope, EnvelopeChoice, Inherited, RustStream};
use crate::loopback::{InFlight, Loopback, Tap};
use crate::partition::{RedisPublishOptions, resolved_headers};
use crate::pipeline::{ListForm, RoundMessage, Window};
use crate::publisher::joins_round;
use crate::recovery::{self, RecoveryConfig};
use crate::{error::RedisError, message::PARTITION_KEY_HEADER};

/// This form's publish policy, [`RedisListPublish`], under the mount-site name every form gives
/// its own. Its options, the envelope and the key TTL, still chain off it.
pub use crate::list::RedisListPublish as Publish;

/// The core prelude, the broker, the [`RedisList`] descriptor and this form's [`Publish`] policy.
/// A list carries no core capability traits.
///
/// # Examples
///
/// ```
/// # mod demo {
/// use ruststream_fred::list::prelude::*;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Deserialize)]
/// struct Job {
///     id: u64,
/// }
///
/// #[derive(Serialize, Outgoing)]
/// #[outgoing(name = "jobs.done")]
/// struct Done {
///     id: u64,
/// }
///
/// #[subscriber(RedisList::new("jobs").reliable(), reply)]
/// async fn run_job(job: &Job) -> Done {
///     Done { id: job.id }
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("jobs", "0.1.0")).with_broker(
///         RedisBroker::standalone("redis://localhost:6379"),
///         |b| {
///             // The reply is `LPUSH`ed onto `jobs.done`, a queue of its own.
///             b.include(run_job).out_reply(Publish::default());
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
/// [`crate::prelude`] and write `list::Publish` there.
pub mod prelude {
    pub use ruststream::prelude::*;

    pub use super::{Publish, RedisList};
    // `keys` arrives as the module, not as a glob: its members are short words a service also uses
    // for its own types, and `Ctx<keys::FredPool>` reads as what it is at the use site.
    pub use crate::context::{PipelineContext, PoolContext, keys};
    pub use crate::pipeline::{Bindable, InRound, RedisPipelineSteps};
    pub use crate::{
        FastStream, Framed, NoOp, PARTITION_KEY_HEADER, RedisBroker, RedisEnvelopeExt,
        RedisPublishOptions, RedisPublishSteps, RedisSubscribeExt, envelope,
    };

    #[cfg(any(
        feature = "tls-rustls",
        feature = "tls-rustls-ring",
        feature = "tls-native-tls"
    ))]
    pub use crate::{TlsConfig, TlsConnector};
}

const DEFAULT_BLOCK: Duration = Duration::from_secs(5);
/// Suffix appended to the list key to form the default processing list (reliable mode).
const PROCESSING_SUFFIX: &str = ".processing";

fn block_secs(block: Duration) -> f64 {
    block.as_secs_f64()
}

/// Normalizes a blocking pop (`BRPOP` / `BLMOVE`) result: fred reports a timed-out pop with nothing
/// available as a timeout error rather than an empty reply, so treat that as "no entry this round"
/// and let the read loop retry. Any other error propagates.
fn empty_on_timeout<T>(
    result: Result<Option<T>, fred::error::Error>,
) -> Result<Option<T>, RedisError> {
    match result {
        Ok(value) => Ok(value),
        Err(err) if matches!(err.kind(), ErrorKind::Timeout) => Ok(None),
        Err(err) => Err(RedisError::stream(err)),
    }
}

/// Describes one list subscription against a [`ConnectedRedisBroker`].
///
/// # Examples
///
/// ```
/// # mod demo {
/// use std::time::Duration;
///
/// use ruststream_fred::list::prelude::*;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Job {
///     id: u64,
/// }
///
/// // Reliable: the entry waits on a processing list until the handler acknowledges it.
/// #[subscriber(RedisList::new("jobs").reliable().block(Duration::from_secs(2)))]
/// async fn run_job(job: &Job) -> HandlerOutcome {
///     tracing::info!(job.id, "running");
///     HandlerOutcome::ack()
/// }
///
/// // Simple: popped and gone, so a crash mid-handler loses the entry.
/// #[subscriber(RedisList::new("metrics"))]
/// async fn record(job: &Job) -> HandlerOutcome {
///     tracing::info!(job.id, "recorded");
///     HandlerOutcome::ack()
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("jobs", "0.1.0")).with_broker(
///         RedisBroker::standalone("redis://localhost:6379"),
///         |b| {
///             b.include(run_job);
///             b.include(record);
///         },
///     )
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Clone)]
#[must_use]
pub struct RedisList<Env = Inherited> {
    key: String,
    reliable: bool,
    processing: Option<String>,
    block: Option<Duration>,
    envelope: Env,
    min_idle: Option<Duration>,
    recovery_zset: Option<String>,
    recovery_ttl: Option<Duration>,
    /// Where the mount site sends a spent delivery, taken from its retry declaration when the
    /// subscription opens, so the broker writes that name as the list it is.
    dead_letter: Option<String>,
}

impl<Env: Debug> Debug for RedisList<Env> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisList")
            .field("key", &self.key)
            .field("reliable", &self.reliable)
            .field("processing", &self.processing)
            .field("envelope", &self.envelope)
            .field("recovery_zset", &self.recovery_zset)
            .field("recovery_ttl", &self.recovery_ttl)
            .field("dead_letter", &self.dead_letter)
            .finish_non_exhaustive()
    }
}

impl RedisList {
    /// A simple (at-most-once) `BRPOP` work-queue consumer on `key`.
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            reliable: false,
            processing: None,
            block: None,
            envelope: Inherited,
            min_idle: None,
            recovery_zset: None,
            recovery_ttl: None,
            dead_letter: None,
        }
    }
}

impl<Env> RedisList<Env> {
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
    ///             let source = RedisList::new("audit").envelope(FastStream);
    ///             b.include(subscriber(source, Log).build());
    ///         },
    ///     )
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    pub fn envelope<Other: Envelope>(self, envelope: Other) -> RedisList<Other> {
        RedisList {
            key: self.key,
            reliable: self.reliable,
            processing: self.processing,
            block: self.block,
            envelope,
            min_idle: self.min_idle,
            recovery_zset: self.recovery_zset,
            recovery_ttl: self.recovery_ttl,
            dead_letter: self.dead_letter,
        }
    }

    /// The envelope this descriptor names, resolved against the broker's when it subscribes.
    pub(crate) const fn envelope_choice(&self) -> &Env {
        &self.envelope
    }

    /// Switches to reliable (at-least-once) mode: entries move to a processing list and are removed
    /// on `ack`.
    pub const fn reliable(mut self) -> Self {
        self.reliable = true;
        self
    }

    /// Sets the processing-list key used in reliable mode. Defaults to `<key>.processing`.
    pub fn processing(mut self, key: impl Into<String>) -> Self {
        self.processing = Some(key.into());
        self
    }

    /// How long one blocking pop waits before looping. Defaults to 5 seconds.
    ///
    /// Also reachable at the mount site, after the batch size, through
    /// [`RedisSubscribeExt`](crate::RedisSubscribeExt).
    pub const fn block(mut self, block: Duration) -> Self {
        self.block = Some(block);
        self
    }

    /// How long a claimed reliable-mode entry may sit idle on the processing list before the
    /// recovery watchdog returns it to the main list. Required for (and only meaningful with)
    /// [`recovery_zset`](Self::recovery_zset).
    ///
    /// It has no default and must exceed the longest legitimate handler runtime: set it too low and
    /// a healthy consumer's in-flight entry gets recovered and processed twice.
    pub const fn min_idle(mut self, min_idle: Duration) -> Self {
        self.min_idle = Some(min_idle);
        self
    }

    /// Opts reliable mode into orphan recovery, naming the ZSET key that tracks in-flight claims.
    ///
    /// Off by default (a dead consumer's entry stays stranded on the processing list). The key has
    /// no sane default, so it is named explicitly here; pair it with [`min_idle`](Self::min_idle),
    /// which is required when recovery is on. Reliable mode is implied. The
    /// [Recovery](https://powersemmi.github.io/ruststream-fred/latest/components/lists/recovery/)
    /// page shows the watchdog in a service.
    ///
    /// # Examples
    ///
    /// ```
    /// # mod demo {
    /// use std::time::Duration;
    ///
    /// use ruststream_fred::list::prelude::*;
    /// use serde::Deserialize;
    ///
    /// #[derive(Deserialize)]
    /// struct Job {
    ///     id: u64,
    /// }
    ///
    /// // At-least-once, with the watchdog that returns a dead consumer's entry to the queue. Set
    /// // `min_idle` above the longest handler runtime, or a running job is recovered and repeated.
    /// #[subscriber(
    ///     RedisList::new("jobs")
    ///         .reliable()
    ///         .min_idle(Duration::from_secs(30))
    ///         .recovery_zset("jobs.inflight")
    /// )]
    /// async fn run_job(job: &Job) -> HandlerOutcome {
    ///     tracing::info!(job.id, "running");
    ///     HandlerOutcome::ack()
    /// }
    ///
    /// #[ruststream::app]
    /// fn app() -> impl App {
    ///     RustStream::new(AppInfo::new("jobs", "0.1.0")).with_broker(
    ///         RedisBroker::standalone("redis://localhost:6379"),
    ///         |b| {
    ///             b.include(run_job);
    ///         },
    ///     )
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    pub fn recovery_zset(mut self, key: impl Into<String>) -> Self {
        self.recovery_zset = Some(key.into());
        self.reliable = true;
        self
    }

    /// An optional auto-cleanup TTL on the recovery ZSET key (refreshed on every claim).
    ///
    /// When set it must exceed [`min_idle`](Self::min_idle) (and the longest legitimate handler
    /// runtime), or in-flight tracking is dropped before the watchdog can act.
    pub const fn recovery_ttl(mut self, ttl: Duration) -> Self {
        self.recovery_ttl = Some(ttl);
        self
    }

    /// The list key this subscription consumes.
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Consumes the definition for its key, so opening a subscription moves the string out
    /// instead of copying it.
    pub(crate) fn into_key(self) -> String {
        self.key
    }

    pub(crate) const fn is_reliable(&self) -> bool {
        self.reliable
    }

    pub(crate) fn processing_or_default(&self) -> String {
        self.processing
            .clone()
            .unwrap_or_else(|| format!("{}{PROCESSING_SUFFIX}", self.key))
    }

    pub(crate) fn block_or_default(&self) -> Duration {
        self.block.unwrap_or(DEFAULT_BLOCK)
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

    /// What this subscription adds to its channel in the generated `AsyncAPI` document, shared by
    /// both broker forms: whether it acknowledges, the processing list it holds claims on, and
    /// how headers are framed beside the payload.
    #[cfg(feature = "asyncapi")]
    fn describe(&self, envelope: crate::asyncapi::Envelope) -> Bindings {
        let processing = self.reliable.then(|| self.processing_or_default());
        crate::asyncapi::channel(&crate::asyncapi::ListSubscription::new(
            self.reliable,
            processing.as_deref(),
            envelope,
        ))
    }

    /// Resolves the recovery settings, or `None` when recovery was not opted into.
    ///
    /// # Errors
    ///
    /// Returns [`RedisError::InvalidOptions`] when a recovery ZSET is named without a
    /// [`min_idle`](Self::min_idle), which has no sane default.
    pub(crate) fn recovery_config(&self) -> Result<Option<RecoveryConfig>, RedisError> {
        let Some(zset_key) = self.recovery_zset.clone() else {
            return Ok(None);
        };
        let min_idle = self.min_idle.ok_or_else(|| {
            RedisError::InvalidOptions(format!(
                "reliable list recovery on `{}` needs a min_idle: call .min_idle(duration) \
                 alongside .recovery_zset(key)",
                self.key
            ))
        })?;
        Ok(Some(RecoveryConfig {
            zset_key,
            min_idle,
            ttl: self.recovery_ttl,
            loopback: Loopback::default(),
        }))
    }
}

// The manual path binds a `Handle` body to its source with `subscriber(source, body)`, which
// takes the descriptor as it is, the way the `#[subscriber(..)]` attribute does.
impl<Env> IntoSource for RedisList<Env> {
    type Source = Self;

    fn into_source(self) -> Self {
        self
    }
}

impl<BrokerEnv, Env> SubscriptionSource<ConnectedRedisBroker<BrokerEnv>> for RedisList<Env>
where
    BrokerEnv: Envelope,
    Env: EnvelopeChoice<BrokerEnv>,
{
    type Subscriber = RedisListSubscriber<Env::Envelope>;
    /// A retry copy is an `LPUSH` this process makes, and the list key is where it goes.
    type Copies = AddressedCopies;

    fn name(&self) -> &str {
        self.key()
    }

    fn declare_retry(self, declaration: &RetryDeclaration) -> Self {
        self.with_declaration(declaration)
    }

    async fn subscribe(
        self,
        connected: &ConnectedRedisBroker<BrokerEnv>,
    ) -> Result<Self::Subscriber, RedisError> {
        connected.subscribe_list(self).await
    }

    #[cfg(feature = "asyncapi")]
    fn channel_bindings(&self) -> Bindings {
        self.describe(crate::asyncapi::Envelope::of::<Env::Envelope>())
    }
}

/// The list key: the subscription pops from it and [`RedisListPublish`] pushes onto it, so a
/// deferred copy lands in the same queue the delivery came from.
impl<BrokerEnv, Env> RedeliveryAddressed<ConnectedRedisBroker<BrokerEnv>> for RedisList<Env>
where
    BrokerEnv: Envelope,
    Env: EnvelopeChoice<BrokerEnv>,
{
    fn redelivery_address(
        &self,
        _connected: &ConnectedRedisBroker<BrokerEnv>,
    ) -> impl Future<Output = Result<RedeliveryAddress, RedisError>> {
        ready(Ok(RedeliveryAddress::new(self.key.clone())))
    }
}

/// A list-backed work-queue subscription.
///
/// A pop returns one entry, so the batches a [`BatchSubscriber`] hands out are assembled on the
/// client by the core's [`BufferedSubscriber`]: it fills a batch up to the size the mount site
/// asked for and closes a partial one on its own deadline.
pub struct RedisListSubscriber<Env = RustStream>(BufferedSubscriber<ListWire<Env>>);

impl<Env: Envelope> Debug for RedisListSubscriber<Env> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("RedisListSubscriber").field(&self.0).finish()
    }
}

impl<Env: Envelope> RedisListSubscriber<Env> {
    pub(crate) fn new(wire: ListWire<Env>) -> Self {
        Self(BufferedSubscriber::new(wire))
    }
}

impl<Env: Envelope> ruststream::Subscriber for RedisListSubscriber<Env> {
    type Message = RedisListMessage<Env>;
    type Error = RedisError;

    /// Yields one message per popped entry.
    ///
    /// # Cancel safety
    ///
    /// Dropping the returned stream between items is safe. In reliable mode an entry already moved
    /// to the processing list but not yet settled stays there until acked or recovered manually.
    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        self.0.stream()
    }
}

impl<Env: Envelope> BatchSubscriber for RedisListSubscriber<Env> {
    type Batch = Vec<RedisListMessage<Env>>;

    /// Yields batches of at most `size` entries, assembled from consecutive pops.
    ///
    /// # Cancel safety
    ///
    /// Same as [`Subscriber::stream`](ruststream::Subscriber::stream). Dropping the stream while a
    /// batch is filling abandons the entries it holds unsettled; in reliable mode they stay on the
    /// processing list until recovered.
    fn batches(
        &mut self,
        size: NonZeroUsize,
    ) -> impl Stream<Item = Result<Self::Batch, Self::Error>> + Send + '_ {
        self.0.batches(size)
    }
}

/// The wire side of a list subscription: one blocking pop per delivery.
/// What every delivery of a list subscription shares: the connection it was read on and the
/// subscription's two keys. One reference per delivery, however many of them it reads.
pub(crate) struct ListShared {
    connection: Arc<Connection>,
    key: String,
    processing: String,
}

pub(crate) struct ListWire<Env> {
    connection: Arc<Connection>,
    /// The connection every pop goes out on, apart from the pool: a blocking pop holds its
    /// connection until it returns.
    reader: ReadConnection,
    key: String,
    reliable: bool,
    processing: String,
    block: Duration,
    envelope: Env,
    recovery: Option<RecoveryConfig>,
    /// The subscription's registration with the in-process server; empty on a real connection.
    tap: Tap,
    /// What every delivery carries back to settle itself.
    shared: Arc<ListShared>,
}

impl<Env> Debug for ListWire<Env> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ListWire")
            .field("key", &self.key)
            .field("reliable", &self.reliable)
            .field("recovery", &self.recovery.is_some())
            .finish_non_exhaustive()
    }
}

impl<Env: Envelope> ListWire<Env> {
    #[allow(
        clippy::too_many_arguments,
        reason = "internal constructor mirroring the descriptor"
    )]
    pub(crate) fn new(
        connection: Arc<Connection>,
        reader: ReadConnection,
        key: String,
        reliable: bool,
        processing: String,
        block: Duration,
        envelope: Env,
        recovery: Option<RecoveryConfig>,
        tap: Tap,
    ) -> Self {
        let shared = Arc::new(ListShared {
            connection: Arc::clone(&connection),
            key: key.clone(),
            processing: processing.clone(),
        });
        Self {
            connection,
            reader,
            key,
            reliable,
            processing,
            block,
            envelope,
            recovery,
            tap,
            shared,
        }
    }

    fn simple_message(&self, raw: Bytes) -> RedisListMessage<Env> {
        RedisListMessage {
            body: Body::open(self.envelope.clone(), raw),
            ack: None,
            shared: Arc::clone(&self.shared),
            flight: self.tap.delivered(),
        }
    }

    fn reliable_message(
        &self,
        raw: Bytes,
        recovery: Option<RecoveryHandle>,
    ) -> RedisListMessage<Env> {
        RedisListMessage {
            body: Body::open(self.envelope.clone(), raw),
            shared: Arc::clone(&self.shared),
            ack: Some(ListAck { recovery }),
            flight: self.tap.delivered(),
        }
    }

    /// Blocks for the next entry, returning `None` when the pop times out (the caller loops). When
    /// recovery is enabled, first returns any orphaned entries to the main list so this same pop can
    /// pick them up.
    async fn next_entry(&self) -> Result<Option<RedisListMessage<Env>>, RedisError> {
        // In process the pop below cannot block, so its wait is taken here, before the recovery
        // sweep, which then finds what went stale while it waited.
        #[cfg(feature = "testing")]
        self.tap.readable().await;
        // The read connection is the subscription's own, so the broker's shutdown does not close
        // it: a read after the shutdown refuses here instead of delivering past it.
        self.connection.ensure_open()?;
        let secs = block_secs(self.block);
        if self.reliable {
            if let Some(cfg) = &self.recovery {
                recovery::sweep_orphans(self.connection.pool(), cfg, &self.key, &self.processing)
                    .await?;
            }
            let value: Option<Bytes> = empty_on_timeout(
                self.reader
                    .client()
                    .blmove(
                        self.key.as_str(),
                        self.processing.as_str(),
                        LMoveDirection::Right,
                        LMoveDirection::Left,
                        secs,
                    )
                    .await,
            )?;
            let Some(value) = value else {
                return Ok(None);
            };
            let handle = match &self.recovery {
                Some(cfg) => {
                    let member =
                        recovery::record_claim(self.connection.pool(), cfg, &value).await?;
                    Some(RecoveryHandle {
                        zset_key: cfg.zset_key.clone(),
                        member,
                    })
                }
                None => None,
            };
            Ok(Some(self.reliable_message(value, handle)))
        } else {
            let popped: Option<(String, Bytes)> =
                empty_on_timeout(self.reader.client().brpop(self.key.as_str(), secs).await)?;
            Ok(popped.map(|(_, v)| self.simple_message(v)))
        }
    }
}

/// One entry a pipelined list read claimed or popped: its raw value and, on a reliable list with
/// orphan recovery, the member its claim is tracked by.
type Claimed = (Bytes, Option<RecoveryHandle>);

impl<Env: Envelope> ListWire<Env> {
    /// The settle side of this subscription inside a window.
    pub(crate) fn round_form(&self) -> ListForm<Env> {
        ListForm::new(
            self.key.as_str(),
            self.processing.as_str(),
            self.recovery.as_ref().map(|cfg| cfg.zset_key.as_str()),
        )
    }

    /// The window's client: a connection of the pool, where no pop blocks.
    pub(crate) fn round_client(&self) -> Client {
        self.shared.connection.pool().next().clone()
    }

    /// The key this subscription reads.
    pub(crate) fn key(&self) -> &str {
        &self.key
    }

    /// Takes up to `count` entries in one round trip, and waits for one when the list is empty.
    ///
    /// A reliable list claims with `LMOVE` into its processing list, `count` of them in one
    /// pipeline, and keeps `BLMOVE` for waiting on an empty queue; a simple list pops with
    /// `RPOP key count` and waits with `BRPOP`. Both read the right end, the end a producer's
    /// `LPUSH` makes the oldest.
    async fn claim(&self, count: usize, into: &mut VecDeque<Claimed>) -> Result<(), RedisError> {
        // The read connection is the subscription's own, so the broker's shutdown does not close
        // it: a read after the shutdown refuses here instead of delivering past it.
        self.connection.ensure_open()?;
        let before = into.len();
        if self.reliable {
            if let Some(cfg) = &self.recovery {
                recovery::sweep_orphans(self.connection.pool(), cfg, &self.key, &self.processing)
                    .await?;
            }
            let claims = self.reader.client().pipeline();
            for _ in 0..count {
                claims
                    .lmove::<(), _, _>(
                        self.key.as_str(),
                        self.processing.as_str(),
                        LMoveDirection::Right,
                        LMoveDirection::Left,
                    )
                    .await
                    .map_err(RedisError::stream)?;
            }
            for reply in claims.try_all::<Value>().await {
                // An empty answer is a pop that found the queue dry; a producer may push between
                // two of them, so every answer is read: each entry answered is on the processing
                // list now and has to be delivered.
                match reply.map_err(RedisError::stream)? {
                    Value::Null => {}
                    value => into.push_back((value_bytes(value)?, None)),
                }
            }
        } else {
            let popped: Value = self
                .reader
                .client()
                .rpop(self.key.as_str(), Some(count))
                .await
                .map_err(RedisError::stream)?;
            if let Value::Array(values) = popped {
                for value in values {
                    into.push_back((value_bytes(value)?, None));
                }
            }
        }
        if into.len() == before
            && let Some(value) = self.wait_one().await?
        {
            // The queue was empty: the blocking pop waited for one, and the next read claims the
            // rest of what arrived.
            into.push_back((value, None));
        }
        self.track(into.range_mut(before..)).await
    }

    /// Waits for one entry on an empty list: `BLMOVE` into the processing list on a reliable list,
    /// `BRPOP` on a simple one. `None` when the wait timed out.
    async fn wait_one(&self) -> Result<Option<Bytes>, RedisError> {
        #[cfg(feature = "testing")]
        self.tap.readable().await;
        let secs = block_secs(self.block);
        if self.reliable {
            empty_on_timeout(
                self.reader
                    .client()
                    .blmove(
                        self.key.as_str(),
                        self.processing.as_str(),
                        LMoveDirection::Right,
                        LMoveDirection::Left,
                        secs,
                    )
                    .await,
            )
        } else {
            let popped: Option<(String, Bytes)> =
                empty_on_timeout(self.reader.client().brpop(self.key.as_str(), secs).await)?;
            Ok(popped.map(|(_, value)| value))
        }
    }

    /// Records every fresh claim in the recovery ZSET, in one pipeline.
    async fn track<'a>(
        &self,
        claimed: impl Iterator<Item = &'a mut Claimed>,
    ) -> Result<(), RedisError> {
        let Some(cfg) = &self.recovery else {
            return Ok(());
        };
        let tracking = self.connection.pool().next().pipeline();
        let mut any = false;
        for (value, handle) in claimed {
            let (score, member) = cfg.tracked(value);
            tracking
                .zadd::<(), _, _>(
                    cfg.zset_key.as_str(),
                    None,
                    None,
                    false,
                    false,
                    (score, member.clone()),
                )
                .await
                .map_err(RedisError::stream)?;
            *handle = Some(RecoveryHandle {
                zset_key: cfg.zset_key.clone(),
                member,
            });
            any = true;
        }
        if !any {
            return Ok(());
        }
        if let Some(ttl) = cfg.ttl_millis() {
            tracking
                .pexpire::<(), _>(cfg.zset_key.as_str(), ttl, None)
                .await
                .map_err(RedisError::stream)?;
        }
        for reply in tracking.try_all::<Value>().await {
            reply.map_err(RedisError::stream)?;
        }
        Ok(())
    }

    fn message_of(&self, (value, recovery): Claimed) -> RedisListMessage<Env> {
        if self.reliable {
            self.reliable_message(value, recovery)
        } else {
            self.simple_message(value)
        }
    }
}

/// The raw bytes of a popped value, as the client holds them.
fn value_bytes(value: Value) -> Result<Bytes, RedisError> {
    match value {
        Value::Bytes(bytes) => Ok(bytes),
        Value::String(text) => Ok(text.into_inner()),
        other => Err(RedisError::Stream(
            format!("a list read answered with {other:?} where an entry was expected").into(),
        )),
    }
}

/// A list subscription's reader, as a pipelined subscription holds it: the wire and the entries
/// its last read claimed.
pub struct ListReader<Env> {
    wire: ListWire<Env>,
    buffer: VecDeque<Claimed>,
}

impl<Env> Debug for ListReader<Env> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ListReader")
            .field("wire", &self.wire)
            .field("buffered", &self.buffer.len())
            .finish()
    }
}

impl<Env: Envelope> ListReader<Env> {
    pub(crate) const fn new(wire: ListWire<Env>) -> Self {
        Self {
            wire,
            buffer: VecDeque::new(),
        }
    }

    /// Yields one delivery per entry, each with a slot in `window`, claiming `count` at a time,
    /// and reports a failed flush of the window once.
    pub(crate) fn round_stream<'a>(
        &'a mut self,
        window: &'a Arc<Window<ListForm<Env>>>,
        count: usize,
    ) -> impl Stream<Item = Result<RoundMessage<ListForm<Env>>, RedisError>> + Send + 'a {
        unfold(self, move |reader| async move {
            loop {
                if let Some(failure) = window.take_failure() {
                    return Some((Err(failure), reader));
                }
                if let Some(claimed) = reader.buffer.pop_front() {
                    let delivery = reader.wire.message_of(claimed);
                    let round = window.open();
                    return Some((
                        Ok(RoundMessage::new(delivery, Arc::clone(window), round)),
                        reader,
                    ));
                }
                if let Err(err) = reader.wire.claim(count, &mut reader.buffer).await {
                    return Some((Err(err), reader));
                }
                window.fetched(reader.buffer.len());
            }
        })
    }
}

impl<Env: Envelope> ListReader<Env> {
    /// Yields one batch per read of up to `size` entries, sharing one slot of `window`, so what
    /// the batch handler queued commits with the settles of every entry.
    pub(crate) fn round_batches<'a>(
        &'a mut self,
        window: &'a Arc<Window<ListForm<Env>>>,
        size: NonZeroUsize,
    ) -> impl Stream<Item = Result<Vec<RoundMessage<ListForm<Env>>>, RedisError>> + Send + 'a {
        window.fill_at(size.get());
        unfold(self, move |reader| async move {
            loop {
                if let Some(failure) = window.take_failure() {
                    return Some((Err(failure), reader));
                }
                if !reader.buffer.is_empty() {
                    let take = size.get().min(reader.buffer.len());
                    let members = u32::try_from(take).unwrap_or(u32::MAX);
                    let round = window.open_batch(members);
                    let batch = reader
                        .buffer
                        .drain(..take)
                        .collect::<Vec<_>>()
                        .into_iter()
                        .map(|claimed| {
                            RoundMessage::new(
                                reader.wire.message_of(claimed),
                                Arc::clone(window),
                                round.clone(),
                            )
                        })
                        .collect();
                    return Some((Ok(batch), reader));
                }
                if let Err(err) = reader.wire.claim(size.get(), &mut reader.buffer).await {
                    return Some((Err(err), reader));
                }
                window.fetched(reader.buffer.len());
            }
        })
    }
}

impl<Env: Envelope> ruststream::Subscriber for ListWire<Env> {
    type Message = RedisListMessage<Env>;
    type Error = RedisError;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        unfold(&*self, |s| async move {
            loop {
                match s.next_entry().await {
                    Ok(Some(msg)) => return Some((Ok(msg), s)),
                    Ok(None) => {}
                    Err(err) => return Some((Err(err), s)),
                }
            }
        })
    }
}

/// Settlement handle for a reliable-mode list delivery.
struct ListAck {
    /// Set when orphan recovery is enabled: the ZSET key and the member tracking this claim, so
    /// settling removes its recovery tracking.
    recovery: Option<RecoveryHandle>,
}

/// The recovery-ZSET coordinates for one in-flight reliable-list claim.
struct RecoveryHandle {
    zset_key: String,
    member: Vec<u8>,
}

/// A list-queue delivery. In simple mode `ack` / `nack` are unsupported; in reliable mode `ack`
/// removes the entry from the processing list and `nack` either returns it or drops it.
pub struct RedisListMessage<Env = RustStream> {
    /// The entry as it arrived, needed verbatim to `LREM` it from the processing list: its payload
    /// located, its headers read when first asked for.
    body: Body<Env>,
    ack: Option<ListAck>,
    /// The connection the entry was read on and the subscription's keys: what settles it, what
    /// `Ctx<keys::FredPool>` hands the handler, and what a warning names.
    shared: Arc<ListShared>,
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

impl<Env: Envelope> RedisListMessage<Env> {
    /// The broker's connection pool, for the per-delivery context.
    pub(crate) fn pool(&self) -> &Pool {
        self.shared.connection.pool()
    }

    /// Takes the harness's count of this delivery, for a window to hold until its flush.
    #[cfg(feature = "testing")]
    pub(crate) fn take_flight(&mut self) -> InFlight {
        std::mem::take(&mut self.flight)
    }

    /// Takes what a window settles this delivery with: the raw entry and the member its claim is
    /// tracked by, on a reliable list. `None` on a simple list, which settles nothing.
    pub(crate) fn into_settle(self) -> Option<(Bytes, Option<Vec<u8>>)> {
        let body = self.body;
        self.ack
            .map(|handle| (body.into_value(), handle.recovery.map(|rec| rec.member)))
    }
}

impl<Env: Envelope> Debug for RedisListMessage<Env> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisListMessage")
            .field("payload_len", &self.body.len())
            .field("reliable", &self.ack.is_some())
            .finish_non_exhaustive()
    }
}

impl<Env: Envelope> IncomingMessage for RedisListMessage<Env> {
    fn payload(&self) -> &[u8] {
        self.body.payload()
    }

    fn headers(&self) -> &HeaderMap {
        self.body.headers(&self.shared.key)
    }

    // See `RedisMessage`: the keyed worker lanes read the key from here, not from `Partitioned`.
    fn partition_key(&self) -> Option<&[u8]> {
        Partitioned::partition_key(self)
    }

    async fn ack(self) -> Result<(), AckError> {
        let Some(handle) = self.ack else {
            return Err(AckError::Unsupported);
        };
        let shared = &self.shared;
        shared.connection.ensure_open_to_settle()?;
        settle(
            shared.connection.pool(),
            &shared.processing,
            &handle,
            self.body.value(),
        )
        .await
    }

    async fn nack(self, requeue: bool) -> Result<(), AckError> {
        let Some(handle) = self.ack else {
            return Err(AckError::Unsupported);
        };
        let shared = &self.shared;
        shared.connection.ensure_open_to_settle()?;
        let pool = shared.connection.pool();
        if requeue {
            // Return the original entry verbatim to the main list, before removing it from
            // processing (a crash in between leaves a duplicate rather than a loss).
            lpush(pool, &shared.key, self.body.value().clone()).await?;
        }
        settle(pool, &shared.processing, &handle, self.body.value()).await
    }
}

fn ack_broker(err: fred::error::Error) -> AckError {
    AckError::Broker(Box::new(err))
}

async fn lpush(pool: &Pool, key: &str, body: Bytes) -> Result<(), AckError> {
    let _: i64 = pool.lpush(key, body).await.map_err(ack_broker)?;
    Ok(())
}

/// Removes the entry from the processing list and, when recovery is enabled, drops its tracking from
/// the recovery ZSET.
async fn settle(
    pool: &Pool,
    processing: &str,
    handle: &ListAck,
    value: &Bytes,
) -> Result<(), AckError> {
    let _: i64 = pool
        .lrem(processing, 1, value.clone())
        .await
        .map_err(ack_broker)?;
    if let Some(rec) = &handle.recovery {
        recovery::forget(pool, &rec.zset_key, &rec.member).await?;
    }
    Ok(())
}

impl<Env: Envelope> Partitioned for RedisListMessage<Env> {
    fn partition_key(&self) -> Option<&[u8]> {
        self.headers().get(PARTITION_KEY_HEADER)
    }
}

/// The declaration half of the list publisher: envelope and key TTL, no connection.
///
/// Constructible anywhere, it pairs into a [`RedisListPublisher`] against a
/// [`ConnectedRedisBroker`].
///
/// # Examples
///
/// ```
/// # mod demo {
/// use std::time::Duration;
///
/// use ruststream::codec::JsonCodec;
/// use ruststream_fred::RedisListPublish;
/// use ruststream_fred::list::prelude::*;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Deserialize)]
/// struct Job {
///     id: u64,
/// }
///
/// #[derive(Serialize, Outgoing)]
/// #[outgoing(name = "jobs.done")]
/// struct Done {
///     id: u64,
/// }
///
/// #[subscriber(RedisList::new("jobs").reliable(), reply)]
/// async fn run_job(job: &Job) -> Done {
///     Done { id: job.id }
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("jobs", "0.1.0")).with_broker(
///         RedisBroker::standalone("redis://localhost:6379"),
///         |b| {
///             // A readable JSON document, and a `jobs.done` nobody drains expires five minutes
///             // after the last push.
///             b.include(run_job).out_reply(
///                 RedisListPublish::new()
///                     .envelope(Framed::new(JsonCodec))
///                     .ttl(Duration::from_secs(300)),
///             );
///         },
///     )
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, Clone)]
#[must_use]
pub struct RedisListPublish<Env = Inherited> {
    envelope: Env,
    ttl: Option<Duration>,
}

impl Default for RedisListPublish {
    fn default() -> Self {
        Self {
            envelope: Inherited,
            ttl: None,
        }
    }
}

impl RedisListPublish {
    /// A policy in the broker's envelope with no key TTL. Equivalent to [`Self::default`].
    pub fn new() -> Self {
        Self::default()
    }
}

impl<Env> RedisListPublish<Env> {
    /// Writes the entries through `envelope` instead of the broker's: how a message's headers sit
    /// beside its payload in the list element. The subscription that drains the list reads the
    /// same envelope.
    ///
    /// # Examples
    ///
    /// ```
    /// # mod demo {
    /// use ruststream_fred::list::prelude::*;
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(Deserialize)]
    /// struct Order {
    ///     id: u64,
    /// }
    ///
    /// #[derive(Serialize, Outgoing)]
    /// #[outgoing(name = "jobs")]
    /// struct Job {
    ///     order: u64,
    /// }
    ///
    /// #[subscriber(RedisList::new("orders"), reply)]
    /// async fn plan(order: &Order) -> Job {
    ///     Job { order: order.id }
    /// }
    ///
    /// #[ruststream::app]
    /// fn app() -> impl App {
    ///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
    ///         RedisBroker::standalone("redis://localhost:6379"),
    ///         |b| {
    ///             // A faststream worker drains `jobs`.
    ///             b.include(plan).out_reply(Publish::new().envelope(FastStream));
    ///         },
    ///     )
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    pub fn envelope<Other: Envelope>(self, envelope: Other) -> RedisListPublish<Other> {
        RedisListPublish {
            envelope,
            ttl: self.ttl,
        }
    }

    /// Sets a time-to-live on the list key, refreshed (`PEXPIRE`) on every publish, so an idle
    /// queue auto-expires. Off by default: without it the list lives until drained or deleted.
    ///
    /// This is a per-key TTL on the whole list, not per-entry: Redis lists have no per-element
    /// expiry, only the key can expire. Each publish pushes the entry and re-arms the key's TTL in
    /// one pipeline, so an actively used queue never expires and only an idle one does. A sub-
    /// millisecond `ttl` is clamped up to 1ms, since `PEXPIRE 0` would delete the key outright.
    pub const fn ttl(mut self, ttl: Duration) -> Self {
        self.ttl = Some(ttl);
        self
    }
}

impl<BrokerEnv, Env> PublishPolicy<ConnectedRedisBroker<BrokerEnv>> for RedisListPublish<Env>
where
    BrokerEnv: Envelope,
    Env: EnvelopeChoice<BrokerEnv>,
{
    type Live = RedisListPublisher<Env::Envelope>;

    fn pair(
        self,
        connected: &ConnectedRedisBroker<BrokerEnv>,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(connected.list_publisher(self)))
    }

    /// The list key the policy pushes onto, the expiry it re-arms there on every push, and how
    /// headers are framed beside the payload.
    #[cfg(feature = "asyncapi")]
    fn channel_bindings(&self, channel: &str) -> Bindings {
        crate::asyncapi::channel(&crate::asyncapi::Publish::list(
            channel,
            self.ttl
                .map(|ttl| u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX)),
            crate::asyncapi::Envelope::of::<Env::Envelope>(),
        ))
    }
}

/// Publishes onto a list with `LPUSH`, so right-popping consumers see FIFO order: a
/// [`RedisListPublish`] policy paired with a connection.
///
/// Obtain it from
/// [`ConnectedRedisBroker::list_publisher`](crate::ConnectedRedisBroker::list_publisher), or by
/// pairing the policy. Publishing after the connection was shut down reports
/// [`RedisError::ShutDown`].
#[derive(Clone)]
pub struct RedisListPublisher<Env = RustStream> {
    core: Arc<RedisCore>,
    envelope: Env,
    ttl: Option<Duration>,
    /// What `pipeline.bind(&out)` names this publisher and its clones by.
    round_name: u64,
}

impl<Env: Debug> Debug for RedisListPublisher<Env> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisListPublisher")
            .field("envelope", &self.envelope)
            .field("ttl", &self.ttl)
            .finish_non_exhaustive()
    }
}

impl<Env: Envelope> RedisListPublisher<Env> {
    /// What `pipeline.bind(&out)` names this publisher and its clones by.
    pub(crate) const fn round_name(&self) -> u64 {
        self.round_name
    }

    pub(crate) fn new<BrokerEnv: Envelope, Choice: EnvelopeChoice<BrokerEnv, Envelope = Env>>(
        core: Arc<RedisCore>,
        publish: RedisListPublish<Choice>,
        broker: &BrokerEnv,
    ) -> Self {
        Self {
            round_name: core.rounds().publisher(),
            core,
            envelope: publish.envelope.resolve(broker),
            ttl: publish.ttl,
        }
    }
}

/// Converts a TTL to the positive millisecond count `PEXPIRE` expects, clamping a sub-millisecond
/// value up to 1 (a `PEXPIRE 0` deletes the key instead of expiring it).
fn ttl_millis(ttl: Duration) -> i64 {
    i64::try_from(ttl.as_millis()).unwrap_or(i64::MAX).max(1)
}

impl<Env: Envelope> ruststream::Publisher for RedisListPublisher<Env> {
    /// As on [`RedisPubSubPublisher`](crate::RedisPubSubPublisher): the entry the client is
    /// handed is an envelope this crate builds, so the payload is only read.
    type Payload = Lend;

    type Error = RedisError;
    /// `LPUSH` carries the key and the value and nothing else; the key TTL is a property of the
    /// queue, fixed by the policy and re-armed on every publish. What a call site still says is
    /// the partition key, framed into the entry with the message's other headers.
    type Options = RedisPublishOptions;

    async fn publish(
        &self,
        msg: ruststream::OutgoingMessage<'_>,
        options: Option<&Self::Options>,
    ) -> Result<(), Self::Error> {
        let (key, payload, headers) = msg.into_parts();
        let mut body = Vec::with_capacity(payload.len());
        self.envelope
            .seal(payload, &resolved_headers(headers, options), &mut body)
            .map_err(|err| RedisError::seal(key, err))?;
        if let Some(round) = self
            .core
            .rounds()
            .joined(self.round_name, joins_round(options))
        {
            return round
                .lpush(key, body, self.ttl.map(ttl_millis))
                .await
                .map_err(RedisError::publish);
        }
        push(&self.core, self.ttl, key, body).await
    }
}

/// `LPUSH`es one sealed entry onto `key`, re-arming the key's `ttl` when there is one: the list
/// publish, shared by [`RedisListPublisher`] and the broker's default publisher.
pub(crate) async fn push(
    core: &RedisCore,
    ttl: Option<Duration>,
    key: &str,
    body: Vec<u8>,
) -> Result<(), RedisError> {
    let pool = core.pool()?;
    let Some(ttl) = ttl else {
        let _: i64 = pool.lpush(key, body).await.map_err(RedisError::publish)?;
        return Ok(());
    };
    // Push the entry and re-arm the key TTL in one pipeline, so an actively used queue keeps
    // resetting its expiry and only an idle one is allowed to lapse.
    let pipeline = pool.next().pipeline();
    let _: () = pipeline
        .lpush(key, body)
        .await
        .map_err(RedisError::publish)?;
    let _: () = pipeline
        .pexpire(key, ttl_millis(ttl), None)
        .await
        .map_err(RedisError::publish)?;
    let _: Vec<Value> = pipeline.all().await.map_err(RedisError::publish)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ttl_millis_converts_and_clamps() {
        assert_eq!(ttl_millis(Duration::from_mins(1)), 60_000);
        assert_eq!(ttl_millis(Duration::from_millis(1)), 1);
        // A sub-millisecond TTL must not become PEXPIRE 0 (which deletes the key).
        assert_eq!(ttl_millis(Duration::from_nanos(1)), 1);
        assert_eq!(ttl_millis(Duration::ZERO), 1);
    }
}
