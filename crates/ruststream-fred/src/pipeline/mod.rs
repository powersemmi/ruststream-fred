//! Consumer-side pipelining: a subscription settles in a window, and a handler queues its own
//! Redis commands into the delivery's segment of that window.
//!
//! `.pipeline()` where the handler is mounted opens the window (see [`RedisPipelineSteps`]): the
//! settles of the deliveries in flight, and the commands their handlers queued through
//! [`keys::Pipeline`](crate::context::keys::Pipeline), leave together, in one pipeline on a
//! connection of the pool, once the read's `COUNT` has settled, once nothing is outstanding, or
//! when the subscription stops. `.atomic()` after it makes each delivery's segment one `MULTI` /
//! `EXEC`.
//!
//! What a handler queued follows the delivery's outcome: `ack` commits it with the settle, and
//! every other outcome discards it.

mod commands;
mod descriptors;
mod forms;
mod message;
mod rounds;
mod source;
#[cfg(all(test, feature = "testing"))]
mod tests;
mod window;

use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use std::ops::Deref;

use fred::bytes_utils::Str;
use fred::clients::{Client, Pipeline};
use fred::error::Error;
use fred::interfaces::ClientLike;
use fred::types::{ClusterHash, CustomCommand, MultipleKeys, MultipleValues, Value};
use ruststream::StartAt;
use ruststream::runtime::{
    Declared, ForReply, Outgoing, PublishContext, PublishTransform, Reads, SubscriberBuilder,
    SubscriberSettings,
};

use crate::partition::RedisPublishOptions;

pub use descriptors::{
    RedisListAtomic, RedisListPipeline, RedisPubSubAtomic, RedisPubSubPatternAtomic,
    RedisPubSubPatternPipeline, RedisPubSubPipeline, RedisStreamAtomic, RedisStreamPipeline,
};
pub(crate) use forms::{ListForm, PubSubForm, StreamForm};
pub use message::RoundMessage;
pub(crate) use rounds::Rounds;
pub use source::PipelinedSubscriber;
pub(crate) use window::{Form, Owner, Round, Segment, Window};

/// A subscription source with a window, as the `.pipeline()` mount step makes it.
///
/// The descriptor is a [`RedisStream`](crate::RedisStream), a [`RedisList`](crate::RedisList), a
/// [`RedisPubSub`](crate::RedisPubSub) or a [`RedisPubSubPattern`](crate::RedisPubSubPattern).
/// `Mode` is [`Plain`] until `.atomic()` makes it [`Atomic`]. A service does not build one: it
/// names the descriptor in `#[subscriber(..)]` and chains [`pipeline`](RedisPipelineSteps::pipeline)
/// where it mounts the handler. The type shows up in the builder's type, under the names
/// [`RedisStreamPipeline`](crate::RedisStreamPipeline) and its siblings.
///
/// # Examples
///
/// ```
/// # mod demo {
/// use ruststream_fred::stream::prelude::*;
/// # #[derive(serde::Deserialize)]
/// # struct Order { id: u64 }
///
/// #[subscriber(RedisStream::new("orders").group("workers"))]
/// async fn record(order: &Order, Ctx(pipeline): Ctx<keys::Pipeline>) -> HandlerOutcome {
///     if pipeline.hset("orders:by-id", (order.id.to_string(), "seen")).await.is_err() {
///         return HandlerOutcome::retry();
///     }
///     HandlerOutcome::ack()
/// }
///
/// fn app() -> RustStream {
///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
///         RedisBroker::standalone("redis://localhost:6379"),
///         |b| {
///             b.include(record.pipeline());
///         },
///     )
/// }
/// # }
/// ```
#[must_use]
pub struct Pipelined<Descriptor, Mode = Plain> {
    descriptor: Descriptor,
    mode: PhantomData<fn() -> Mode>,
}

impl<Descriptor: Clone, Mode> Clone for Pipelined<Descriptor, Mode> {
    fn clone(&self) -> Self {
        Self {
            descriptor: self.descriptor.clone(),
            mode: PhantomData,
        }
    }
}

impl<Descriptor: Debug, Mode: WindowMode> Debug for Pipelined<Descriptor, Mode> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pipelined")
            .field("descriptor", &self.descriptor)
            .field("atomic", &Mode::ATOMIC)
            .finish()
    }
}

impl<Descriptor, Mode> Pipelined<Descriptor, Mode> {
    pub(crate) const fn wrap(descriptor: Descriptor) -> Self {
        Self {
            descriptor,
            mode: PhantomData,
        }
    }

    /// The descriptor the window was opened on.
    pub(crate) const fn descriptor(&self) -> &Descriptor {
        &self.descriptor
    }

    pub(crate) fn into_descriptor(self) -> Descriptor {
        self.descriptor
    }

    /// The same window over the descriptor `step` returns: a base setting chained at the mount
    /// site after `.pipeline()`.
    pub(crate) fn map(self, step: impl FnOnce(Descriptor) -> Descriptor) -> Self {
        Self::wrap(step(self.descriptor))
    }
}

/// The window's mode before `.atomic()`: a segment is the handler's commands, in the window's
/// pipeline.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Plain;

/// The window's mode after `.atomic()`: a segment is `MULTI`, the handler's commands, the settle,
/// `EXEC`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Atomic;

mod sealed {
    pub trait Sealed {}
    impl Sealed for super::Plain {}
    impl Sealed for super::Atomic {}
}

/// The two modes of a window, [`Plain`] and [`Atomic`].
///
/// # Examples
///
/// ```
/// use ruststream_fred::pipeline::{Atomic, Plain, WindowMode};
///
/// assert!(!Plain::ATOMIC);
/// assert!(Atomic::ATOMIC);
/// ```
pub trait WindowMode: sealed::Sealed + Send + Sync + 'static {
    /// Whether a segment is wrapped in `MULTI` / `EXEC`.
    const ATOMIC: bool;
}

impl WindowMode for Plain {
    const ATOMIC: bool = false;
}

impl WindowMode for Atomic {
    const ATOMIC: bool = true;
}

mod windowable {
    /// The descriptors a window opens on, and nothing else.
    #[diagnostic::on_unimplemented(
        message = "`.pipeline()` opens a window on a `RedisStream`, `RedisList`, `RedisPubSub` or \
                   `RedisPubSubPattern` subscription, and this one is `{Self}`",
        label = "no window opens here",
        note = "a subscription has one window: chain `.pipeline()` once, on the descriptor the \
                attribute names"
    )]
    pub trait Base {}
    impl Base for crate::RedisStream {}
    impl Base for crate::RedisList {}
    impl Base for crate::RedisPubSub {}
    impl Base for crate::RedisPubSubPattern {}
}

/// A subscription source a window can open on: a descriptor of this crate, possibly under the
/// core's `start_at(..)` wrapper.
#[diagnostic::on_unimplemented(
    message = "`.pipeline()` opens a window on a subscription of ruststream-fred, and `{Self}` is not one",
    label = "no window opens on this source",
    note = "name a `RedisStream`, `RedisList`, `RedisPubSub` or `RedisPubSubPattern` in \
            `#[subscriber(..)]`, and chain `.pipeline()` once"
)]
pub trait OpensWindow {
    /// The same source with a window.
    type Pipelined;

    /// Opens the window.
    fn open_window(self) -> Self::Pipelined;
}

impl<Descriptor: windowable::Base> OpensWindow for Descriptor {
    type Pipelined = Pipelined<Descriptor, Plain>;

    fn open_window(self) -> Self::Pipelined {
        Pipelined::wrap(self)
    }
}

impl<Source: OpensWindow, Position> OpensWindow for StartAt<Source, Position> {
    type Pipelined = StartAt<Source::Pipelined, Position>;

    fn open_window(self) -> Self::Pipelined {
        self.map_inner(OpensWindow::open_window)
    }
}

/// A source that already has a window, which is what `.atomic()` needs.
#[diagnostic::on_unimplemented(
    message = "`.atomic()` needs `.pipeline()` first",
    label = "this subscription has no pipeline",
    note = "write `.pipeline().atomic()`: a transaction is one segment of the window's pipeline, \
            and a subscription without a window has no pipeline to put it in"
)]
pub trait Windowed {
    /// The same source with an atomic window.
    type Atomic;

    /// Makes the window atomic.
    fn into_atomic(self) -> Self::Atomic;
}

impl<Descriptor> Windowed for Pipelined<Descriptor, Plain> {
    type Atomic = Pipelined<Descriptor, Atomic>;

    fn into_atomic(self) -> Self::Atomic {
        Pipelined::wrap(self.descriptor)
    }
}

impl<Source: Windowed, Position> Windowed for StartAt<Source, Position> {
    type Atomic = StartAt<Source::Atomic, Position>;

    fn into_atomic(self) -> Self::Atomic {
        self.map_inner(Windowed::into_atomic)
    }
}

/// The builder step behind [`RedisPipelineSteps::pipeline`].
#[diagnostic::on_unimplemented(
    message = "`.pipeline()` opens a window on a subscription of ruststream-fred, and this \
               registration is not one",
    label = "no window opens on this registration",
    note = "name a `RedisStream`, `RedisList`, `RedisPubSub` or `RedisPubSubPattern` in \
            `#[subscriber(..)]`, and chain `.pipeline()` once"
)]
pub trait PipelineStep {
    /// The builder over the pipelined source.
    type Out;

    /// Opens the window on the builder's source.
    fn apply_pipeline(self) -> Self::Out;
}

impl<Def, Source, State, DefCodec> PipelineStep for SubscriberBuilder<Def, Source, State, DefCodec>
where
    Def: Declared,
    Source: OpensWindow,
{
    type Out = SubscriberBuilder<Def, Source::Pipelined, State, DefCodec>;

    fn apply_pipeline(self) -> Self::Out {
        self.map_source(OpensWindow::open_window)
    }
}

/// The builder step behind [`RedisPipelineSteps::atomic`].
#[diagnostic::on_unimplemented(
    message = "`.atomic()` needs `.pipeline()` first",
    label = "this subscription has no pipeline",
    note = "write `.pipeline().atomic()`: a transaction is one segment of the window's pipeline, \
            and a subscription without a window has no pipeline to put it in"
)]
pub trait AtomicStep {
    /// The builder over the source with an atomic window.
    type Out;

    /// Makes the window atomic.
    fn apply_atomic(self) -> Self::Out;
}

impl<Def, Source, State, DefCodec> AtomicStep for SubscriberBuilder<Def, Source, State, DefCodec>
where
    Def: Declared,
    Source: Windowed,
{
    type Out = SubscriberBuilder<Def, Source::Atomic, State, DefCodec>;

    fn apply_atomic(self) -> Self::Out {
        self.map_source(Windowed::into_atomic)
    }
}

/// The window steps of a mount site: `.pipeline()` opens a consumer window on the subscription,
/// and `.atomic()` after it makes each delivery's segment one `MULTI` / `EXEC`.
///
/// The steps chain on the handler where it is mounted, like the core's settings, after the
/// descriptor the attribute named and after any base setting such as
/// [`block`](crate::RedisSubscribeExt::block). They apply to a [`RedisStream`](crate::RedisStream),
/// a [`RedisList`](crate::RedisList), a [`RedisPubSub`](crate::RedisPubSub) and a
/// [`RedisPubSubPattern`](crate::RedisPubSubPattern) alike; see [the module](self) for what the
/// window does.
///
/// Under `.atomic()` a delivery's segment is `MULTI`, the commands its handler queued, the
/// delivery's settle where the form has one, and `EXEC`, sent inside the window's pipeline, so the
/// handler's Redis side effects and its acknowledgement happen together or not at all. It buys no
/// rollback: Redis has none, and a command that fails inside `EXEC` (a `WRONGTYPE`, say) leaves
/// the others executed.
///
/// On a cluster, Redis runs a transaction only when every key of it hashes to one slot. A
/// segment's slot is the subscription key's, so a handler's keys share it through a hash tag
/// (`{orders}:invoices` beside a stream `{orders}`). A command for another slot is answered with a
/// redirection inside the transaction, and Redis then refuses the whole `EXEC`, so a segment is
/// never executed in part.
///
/// A handler that reads `Ctx<keys::Pipeline>` needs `.pipeline()` at its mount site. Without it
/// the mount does not compile, and the error is the framework's general one about the handler's
/// context not matching the subscription's (`PipelineContext: BuildContext<RedisMessage>`).
///
/// # Examples
///
/// ```
/// # mod demo {
/// use ruststream_fred::stream::prelude::*;
/// # #[derive(serde::Deserialize)]
/// # struct Order { id: u64 }
///
/// #[subscriber(RedisStream::new("{orders}").group("workers"))]
/// async fn invoice(order: &Order, Ctx(pipeline): Ctx<keys::Pipeline>) -> HandlerOutcome {
///     if pipeline.lpush("{orders}:invoices", order.id.to_string()).await.is_err() {
///         return HandlerOutcome::retry();
///     }
///     HandlerOutcome::ack()
/// }
///
/// fn app() -> RustStream {
///     RustStream::new(AppInfo::new("invoices", "0.1.0")).with_broker(
///         RedisBroker::standalone("redis://localhost:6379"),
///         |b| {
///             b.include(invoice.pipeline().atomic());
///         },
///     )
/// }
/// # }
/// ```
///
/// A subscription without a window has nothing to make atomic:
///
/// ```compile_fail
/// # mod demo {
/// use ruststream_fred::stream::prelude::*;
/// # #[derive(serde::Deserialize)]
/// # struct Order { id: u64 }
///
/// #[subscriber(RedisStream::new("orders").group("workers"))]
/// async fn invoice(order: &Order) -> HandlerOutcome {
///     let _ = order.id;
///     HandlerOutcome::ack()
/// }
///
/// fn app() -> RustStream {
///     RustStream::new(AppInfo::new("invoices", "0.1.0")).with_broker(
///         RedisBroker::standalone("redis://localhost:6379"),
///         |b| {
///             b.include(invoice.atomic());
///         },
///     )
/// }
/// # }
/// ```
pub trait RedisPipelineSteps: Declared {
    /// Opens a consumer window on the subscription: its settles and the commands its handlers
    /// queue through [`keys::Pipeline`](crate::context::keys::Pipeline) leave together, in one
    /// pipeline on a connection of the pool.
    ///
    /// On a stream the window flushes once the read's `COUNT` has settled, once nothing is
    /// outstanding, or when the subscription stops: under load one `XACK` per fetched batch. A
    /// pipelined list reads in batches, and a reliable list's `LREM` settles ride the window.
    /// Pub/Sub, on a channel or a pattern, settles nothing, so its window carries the handlers'
    /// commands alone, and leaves once nothing is in flight.
    fn pipeline(self) -> <Self::Settings as PipelineStep>::Out
    where
        Self::Settings: PipelineStep,
    {
        self.declare().apply_pipeline()
    }

    /// Makes each delivery's segment one `MULTI` / `EXEC`. Follows `.pipeline()`.
    fn atomic(self) -> <Self::Settings as AtomicStep>::Out
    where
        Self::Settings: AtomicStep,
    {
        self.declare().apply_atomic()
    }
}

impl<D: Declared> RedisPipelineSteps for D {}

mod bindable {
    /// What a publisher of this crate is named by in a binding.
    pub trait Named {
        fn round_name(&self) -> u64;
    }
}

/// A publisher of this crate, which `pipeline.bind(&out)` can bind to a delivery's round.
///
/// Implemented by every publisher this crate's policies pair into, and by nothing else: a
/// publisher of another broker publishes through its own connection, which no round of this one
/// can carry. A handler bounds its slot with it, `Out(out): Out<impl Bindable>`, to bind it.
///
/// # Examples
///
/// ```
/// # mod demo {
/// use ruststream_fred::stream::prelude::*;
/// # #[derive(serde::Deserialize)]
/// # struct Order { id: u64 }
/// #[derive(serde::Serialize, Outgoing)]
/// #[outgoing(name = "audit")]
/// struct Audit {
///     id: u64,
/// }
///
/// /// The audit entry leaves with the delivery's acknowledgement, and not without it.
/// #[subscriber(RedisStream::new("orders").group("workers"))]
/// async fn record(
///     order: &Order,
///     Ctx(pipeline): Ctx<keys::Pipeline>,
///     Out(out): Out<impl Bindable>,
/// ) -> HandlerOutcome {
///     let out = pipeline.bind(out);
///     if out.message(&Audit { id: order.id }).publish().await.is_err() {
///         return HandlerOutcome::retry();
///     }
///     HandlerOutcome::ack()
/// }
///
/// fn app() -> RustStream {
///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
///         RedisBroker::standalone("redis://localhost:6379"),
///         |b| {
///             b.include(record.pipeline()).out(DefaultSlot, Publish).build();
///         },
///     )
/// }
/// # }
/// ```
///
/// A publisher of another transport has no round to join, so binding one does not compile:
///
/// ```compile_fail,E0277
/// use std::convert::Infallible;
///
/// use ruststream::{Lend, OutgoingMessage, Publisher};
/// use ruststream_fred::pipeline::Bindable;
///
/// struct Elsewhere;
///
/// impl Publisher for Elsewhere {
///     type Payload = Lend;
///     type Error = Infallible;
///     type Options = ();
///
///     async fn publish(
///         &self,
///         _msg: OutgoingMessage<'_, &[u8]>,
///         _options: Option<&()>,
///     ) -> Result<(), Infallible> {
///         Ok(())
///     }
/// }
///
/// fn bindable<P: Bindable>() {}
///
/// bindable::<Elsewhere>();
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a publisher of ruststream-fred, so it cannot join a delivery's round",
    label = "this slot publishes through another broker",
    note = "bind a slot whose policy is one of this crate's (`stream::Publish`, `list::Publish`, \
            `pubsub::Publish`), and bound the slot parameter as `Out<impl Bindable>`"
)]
pub trait Bindable: ruststream::Publisher + bindable::Named {}

impl<T: ruststream::Publisher + bindable::Named> Bindable for T {}

impl bindable::Named for crate::RedisPublisher {
    fn round_name(&self) -> u64 {
        self.round_name()
    }
}

impl bindable::Named for crate::RedisListPublisher {
    fn round_name(&self) -> u64 {
        self.round_name()
    }
}

impl bindable::Named for crate::RedisPubSubPublisher {
    fn round_name(&self) -> u64 {
        self.round_name()
    }
}

impl bindable::Named for crate::RedisDefaultPublisher {
    fn round_name(&self) -> u64 {
        self.round_name()
    }
}

/// The reply transform that puts a reply in the round of the delivery it answers.
///
/// A reply is published after the handler returns and before the delivery settles. Mounted on
/// the reply position of a `.pipeline()` subscription, this transform queues it into the
/// delivery's segment instead: it leaves with the window, after what the handler queued, and
/// under `.atomic()` inside the delivery's `MULTI` / `EXEC`. On a subscription without a window
/// the reply leaves at once, as it does without the transform.
///
/// # Examples
///
/// ```
/// # mod demo {
/// use ruststream_fred::stream::prelude::*;
/// # #[derive(serde::Deserialize)]
/// # struct Order { id: u64 }
/// #[derive(serde::Serialize, Outgoing)]
/// #[outgoing(name = "receipts")]
/// struct Receipt {
///     id: u64,
/// }
///
/// #[subscriber(RedisStream::new("orders").group("workers"), publish)]
/// async fn issue(order: &Order) -> Receipt {
///     Receipt { id: order.id }
/// }
///
/// fn app() -> RustStream {
///     RustStream::new(AppInfo::new("receipts", "0.1.0")).with_broker(
///         RedisBroker::standalone("redis://localhost:6379"),
///         |b| {
///             b.include(issue.pipeline()).out_reply(Publish).transform(InRound);
///         },
///     )
/// }
/// # }
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct InRound;

impl<C> PublishTransform<ForReply<C>, RedisPublishOptions> for InRound {
    type Destination = Reads;

    fn apply(
        &self,
        _out: &mut Outgoing<'_>,
        options: &mut Option<RedisPublishOptions>,
        _cx: &PublishContext<'_, C>,
    ) {
        options
            .get_or_insert_with(RedisPublishOptions::default)
            .join_round = true;
    }
}

/// The delivery's own buffer in the window: what a handler queues its Redis commands into.
///
/// Reached as `Ctx<keys::Pipeline>` on a `.pipeline()` subscription. It carries `fred`'s command
/// methods, each queueing the command and returning `Ok(())` once it is queued: the command runs
/// after the handler has returned, and only if the delivery is acknowledged, so its reply is not
/// available inside the handler. A command queued before `ack` executes before that `ack`. The
/// window sends it; there is no method here that sends. For a command whose answer the handler needs now, take
/// `Ctx<keys::FredPool>`.
///
/// The buffer is created on the first queued command, so a delivery whose handler queues nothing
/// costs the window nothing.
///
/// # Examples
///
/// ```
/// # mod demo {
/// use ruststream_fred::stream::prelude::*;
/// # #[derive(serde::Deserialize)]
/// # struct Order { id: u64 }
///
/// /// Records the order and acknowledges it in one round trip with the rest of the window.
/// #[subscriber(RedisStream::new("orders").group("workers"))]
/// async fn record(order: &Order, Ctx(pipeline): Ctx<keys::Pipeline>) -> HandlerOutcome {
///     if pipeline.hset("orders:by-id", (order.id.to_string(), "seen")).await.is_err() {
///         return HandlerOutcome::retry();
///     }
///     HandlerOutcome::ack()
/// }
///
/// fn app() -> RustStream {
///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
///         RedisBroker::standalone("redis://localhost:6379"),
///         |b| {
///             b.include(record.pipeline());
///         },
///     )
/// }
/// # }
/// ```
///
/// A mount site without `.pipeline()` has no round to hand out, so the mount does not compile:
///
/// ```compile_fail
/// # mod demo {
/// use ruststream::prelude::*;
/// use ruststream::subscriber;
/// use ruststream_fred::context::keys;
/// use ruststream_fred::RedisStream;
/// # #[derive(serde::Deserialize)]
/// # struct Order { id: u64 }
///
/// #[subscriber(RedisStream::new("orders").group("workers"))]
/// async fn record(order: &Order, Ctx(pipeline): Ctx<keys::Pipeline>) -> HandlerOutcome {
///     let _ = (order, pipeline);
///     HandlerOutcome::ack()
/// }
///
/// fn app() -> RustStream {
///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
///         ruststream_fred::RedisBroker::standalone("redis://localhost:6379"),
///         |b| {
///             b.include(record);
///         },
///     )
/// }
/// # }
/// ```
#[derive(Clone)]
pub struct RedisPipeline {
    round: Round,
}

impl Debug for RedisPipeline {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisPipeline").finish_non_exhaustive()
    }
}

impl RedisPipeline {
    pub(crate) const fn new(round: Round) -> Self {
        Self { round }
    }

    /// The delivery's segment, created on first use.
    async fn segment(&self) -> Result<Segment, Error> {
        self.round.segment().await
    }

    /// Binds a slot's publisher to this delivery's round, and hands the slot back.
    ///
    /// What the slot publishes from this delivery's handler is queued into the delivery's segment
    /// from then on: it leaves with the window, after what was queued before it, only if the
    /// delivery is acknowledged, and under `.atomic()` inside the delivery's `MULTI` / `EXEC`.
    /// The slot keeps its codec and its transforms. A slot publish the handler does not bind
    /// leaves at once.
    ///
    /// See [`Bindable`] for a handler that binds its slot.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::ops::Deref;
    /// use ruststream_fred::pipeline::{Bindable, RedisPipeline};
    ///
    /// fn bound<'o, O>(pipeline: &RedisPipeline, out: &'o O) -> &'o O
    /// where
    ///     O: Deref,
    ///     O::Target: Bindable,
    /// {
    ///     pipeline.bind(out)
    /// }
    /// # let _ = bound::<Box<ruststream_fred::RedisPublisher>>;
    /// ```
    pub fn bind<'o, O>(&self, out: &'o O) -> &'o O
    where
        O: Deref,
        O::Target: Bindable,
    {
        self.round.bind(bindable::Named::round_name(&**out));
        out
    }

    /// Queues a Lua script run by its SHA1 digest (`EVALSHA`), with its keys and arguments.
    ///
    /// Queued into this delivery's segment; it runs after the handler returns, when the delivery
    /// is acknowledged. On a cluster the command goes to the node of the first key.
    ///
    /// # Errors
    ///
    /// Returns `fred`'s error when the arguments do not convert, or when the delivery has already
    /// settled.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_fred::pipeline::RedisPipeline;
    ///
    /// async fn run(pipeline: &RedisPipeline, sha: &str) -> Result<(), fred::error::Error> {
    ///     pipeline.evalsha(sha, vec!["orders:seen"], vec!["1"]).await
    /// }
    /// # let _ = run;
    /// ```
    pub async fn evalsha<S, K, V>(&self, hash: S, keys: K, args: V) -> Result<(), Error>
    where
        S: Into<Str> + Send,
        K: Into<MultipleKeys> + Send,
        V: TryInto<MultipleValues> + Send,
        V::Error: Into<Error> + Send,
    {
        let args = args.try_into().map_err(Into::into)?;
        self.script("EVALSHA", hash.into(), keys.into(), args).await
    }

    /// Queues a Lua script run by its source (`EVAL`), with its keys and arguments.
    ///
    /// Queued into this delivery's segment; it runs after the handler returns, when the delivery
    /// is acknowledged. On a cluster the command goes to the node of the first key.
    ///
    /// # Errors
    ///
    /// Returns `fred`'s error when the arguments do not convert, or when the delivery has already
    /// settled.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_fred::pipeline::RedisPipeline;
    ///
    /// async fn run(pipeline: &RedisPipeline) -> Result<(), fred::error::Error> {
    ///     let script = "return redis.call('INCR', KEYS[1])";
    ///     pipeline.eval(script, vec!["orders:seen"], Vec::<String>::new()).await
    /// }
    /// # let _ = run;
    /// ```
    pub async fn eval<S, K, V>(&self, script: S, keys: K, args: V) -> Result<(), Error>
    where
        S: Into<Str> + Send,
        K: Into<MultipleKeys> + Send,
        V: TryInto<MultipleValues> + Send,
        V::Error: Into<Error> + Send,
    {
        let args = args.try_into().map_err(Into::into)?;
        self.script("EVAL", script.into(), keys.into(), args).await
    }

    /// A script run as `fred` sends one: the script or its digest, the key count, the keys and
    /// the arguments, routed by the first key.
    async fn script(
        &self,
        command: &'static str,
        body: Str,
        keys: MultipleKeys,
        args: Value,
    ) -> Result<(), Error> {
        let keys = keys.inner();
        let hash = keys.first().map_or(ClusterHash::Random, |key| {
            ClusterHash::Custom(key.cluster_hash())
        });
        let mut values = Vec::with_capacity(2 + keys.len());
        values.push(Value::from(body));
        values.push(Value::from(i64::try_from(keys.len()).unwrap_or(i64::MAX)));
        values.extend(keys.into_iter().map(Value::from));
        match args {
            Value::Array(args) => values.extend(args),
            Value::Null => {}
            arg => values.push(arg),
        }
        self.custom(CustomCommand::new_static(command, hash, false), values)
            .await
    }

    /// Queues a command this facade does not name, by its name and its arguments.
    ///
    /// Queued into this delivery's segment; it runs after the handler returns, when the delivery
    /// is acknowledged.
    ///
    /// # Errors
    ///
    /// Returns `fred`'s error when the arguments do not convert, or when the delivery has already
    /// settled.
    ///
    /// # Examples
    ///
    /// ```
    /// use fred::types::{ClusterHash, CustomCommand};
    /// use ruststream_fred::pipeline::RedisPipeline;
    ///
    /// async fn touch(pipeline: &RedisPipeline) -> Result<(), fred::error::Error> {
    ///     let command = CustomCommand::new_static("TOUCH", ClusterHash::FirstKey, false);
    ///     pipeline.custom(command, vec!["orders:seen"]).await
    /// }
    /// # let _ = touch;
    /// ```
    pub async fn custom<T>(&self, command: CustomCommand, args: Vec<T>) -> Result<(), Error>
    where
        T: TryInto<Value> + Send,
        T::Error: Into<Error> + Send,
    {
        match self.segment().await? {
            Segment::Plain(segment) => segment.custom::<(), T>(command, args).await,
            Segment::Atomic(segment) => segment.custom::<(), T>(command, args).await,
        }
    }
}

/// A fresh `fred` pipeline on `client`, for one segment or one flush's settles.
pub(crate) fn pipeline_on(client: &Client) -> Pipeline<Client> {
    client.pipeline()
}
