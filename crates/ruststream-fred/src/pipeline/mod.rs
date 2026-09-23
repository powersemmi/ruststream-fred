//! Consumer-side pipelining: a subscription settles in a window, and a handler queues its own
//! Redis commands into the delivery's segment of that window.
//!
//! `.pipeline()` on a descriptor opens the window: the settles of the deliveries in flight, and the
//! commands their handlers queued through [`keys::Pipeline`](crate::context::keys::Pipeline), leave
//! together, in one pipeline on a connection of the pool, once the read's `COUNT` has settled,
//! once nothing is outstanding, or when the subscription stops. `.atomic()` after it makes each
//! delivery's segment one `MULTI` / `EXEC`.
//!
//! What a handler queued follows the delivery's outcome: `ack` commits it with the settle, and
//! every other outcome discards it.

mod commands;
mod descriptors;
mod forms;
mod message;
mod source;
mod window;

use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;

use fred::clients::{Client, Pipeline};
use fred::error::{Error, ErrorKind};
use fred::interfaces::ClientLike;
use fred::types::{CustomCommand, FromValue, Value};

pub use descriptors::{
    AtomicList, AtomicPubSub, AtomicStream, PipelinedList, PipelinedPubSub, PipelinedStream,
};
#[cfg(feature = "testing")]
pub(crate) use forms::testing::TestForm;
pub(crate) use forms::{PubSubForm, StreamForm};
pub use message::RoundMessage;
pub use source::PipelinedSubscriber;
pub(crate) use window::{Form, Round, Segment, Window};

/// A subscription descriptor with a window, as its `.pipeline()` step returns it.
///
/// The descriptor is a [`RedisStream`](crate::RedisStream), a [`RedisList`](crate::RedisList) or
/// a [`RedisPubSub`](crate::RedisPubSub). `Mode` is [`Plain`] until `.atomic()` makes it
/// [`Atomic`].
///
/// # Examples
///
/// ```
/// use ruststream_fred::RedisStream;
/// use ruststream_fred::pipeline::AtomicStep;
///
/// let windowed = RedisStream::new("orders").group("workers").pipeline();
/// let atomic = RedisStream::new("orders").group("workers").pipeline().atomic();
/// # let _ = (windowed, atomic);
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
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_fred::RedisStream;
    ///
    /// let windowed = RedisStream::new("orders").group("workers").pipeline();
    /// assert_eq!(windowed.descriptor().key(), "orders");
    /// ```
    pub const fn descriptor(&self) -> &Descriptor {
        &self.descriptor
    }

    pub(crate) fn into_descriptor(self) -> Descriptor {
        self.descriptor
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

/// A descriptor that already has a window, which is what `.atomic()` needs.
#[diagnostic::on_unimplemented(
    message = "`.atomic()` needs `.pipeline()` first",
    label = "this subscription has no pipeline",
    note = "write `.pipeline().atomic()`: a transaction is one segment of the window's pipeline, \
            and a subscription without a window has no pipeline to put it in"
)]
pub trait Windowed {
    /// The same descriptor with an atomic window.
    type Atomic;

    /// Makes the window atomic.
    fn into_atomic(self) -> Self::Atomic;
}

impl<Descriptor> Windowed for Pipelined<Descriptor, Plain> {
    type Atomic = Pipelined<Descriptor, Atomic>;

    fn into_atomic(self) -> Self::Atomic {
        Pipelined {
            descriptor: self.descriptor,
            mode: PhantomData,
        }
    }
}

/// The `.atomic()` step of a subscription descriptor: it follows `.pipeline()`.
///
/// Under it a delivery's segment is `MULTI`, the commands its handler queued, the delivery's
/// settle where the form has one, and `EXEC`, sent inside the window's pipeline, so the handler's
/// Redis side effects and its acknowledgement happen together or not at all. It buys no rollback:
/// Redis has none, and a command that fails inside `EXEC` (a `WRONGTYPE`, say) leaves the others
/// executed.
///
/// On a cluster, Redis runs a transaction only when every key of it hashes to one slot. A
/// segment's slot is the subscription key's, so a handler's keys share it through a hash tag
/// (`{orders}:invoices` beside a stream `{orders}`). A command for another slot is answered with a
/// redirection inside the transaction, and Redis then refuses the whole `EXEC`, so a segment is
/// never executed in part.
///
/// # Examples
///
/// ```
/// use ruststream_fred::RedisStream;
/// use ruststream_fred::pipeline::AtomicStep;
///
/// let orders = RedisStream::new("{orders}").group("workers").pipeline().atomic();
/// # let _ = orders;
/// ```
///
/// A subscription without a window has nothing to make atomic:
///
/// ```compile_fail
/// use ruststream_fred::RedisStream;
/// use ruststream_fred::pipeline::AtomicStep;
///
/// let orders = RedisStream::new("orders").group("workers").atomic();
/// ```
pub trait AtomicStep: Sized {
    /// Makes each delivery's segment one `MULTI` / `EXEC`.
    fn atomic(self) -> Self::Atomic
    where
        Self: Windowed,
    {
        self.into_atomic()
    }
}

impl AtomicStep for crate::RedisStream {}
impl AtomicStep for crate::RedisList {}
impl AtomicStep for crate::RedisPubSub {}
impl<Descriptor, Mode> AtomicStep for Pipelined<Descriptor, Mode> {}

/// The delivery's own buffer in the window: what a handler queues its Redis commands into.
///
/// Reached as `Ctx<keys::Pipeline>` on a `.pipeline()` subscription. It carries `fred`'s command
/// methods, each queueing the command and returning `()`: the command runs after the handler has
/// returned, and only if the delivery is acknowledged, so its reply is not available inside the
/// handler. A command queued before `ack` executes before that `ack`. The window sends it; there
/// is no method here that sends. For a command whose answer the handler needs now, take
/// `Ctx<keys::FredPool>`.
///
/// The buffer is created on the first queued command, so a delivery whose handler queues nothing
/// costs the window nothing.
///
/// # Examples
///
/// ```
/// # mod demo {
/// use ruststream::prelude::*;
/// use ruststream::subscriber;
/// use ruststream_fred::PipelinedStream;
/// use ruststream_fred::context::keys;
/// # #[derive(serde::Deserialize)]
/// # struct Order { id: u64 }
///
/// /// Records the order and acknowledges it in one round trip with the rest of the window.
/// #[subscriber(PipelinedStream::new("orders").group("workers"))]
/// async fn record(order: &Order, Ctx(pipeline): Ctx<keys::Pipeline>) -> HandlerOutcome {
///     if pipeline.hset("orders:by-id", (order.id.to_string(), "seen")).await.is_err() {
///         return HandlerOutcome::retry();
///     }
///     HandlerOutcome::ack()
/// }
/// # }
/// ```
///
/// A subscription without `.pipeline()` has no round to hand out:
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
    fn segment(&self) -> Result<Segment, Error> {
        self.round.segment()
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
        match self.segment()? {
            Segment::Plain(segment) => segment.custom::<(), T>(command, args).await,
            Segment::Atomic(segment) => segment.custom::<(), T>(command, args).await,
        }
    }
}

/// Queues `command` into a `fred` buffer and reads the reply it answers with at once: a pipeline
/// or a transaction answers `QUEUED` as the command lands in its buffer, so the future is ready on
/// its first poll and the window can queue its own commands while it holds its lock.
pub(crate) fn queued<R: FromValue>(
    command: impl Future<Output = Result<R, Error>>,
) -> Result<R, Error> {
    use futures::FutureExt;
    command.now_or_never().unwrap_or_else(|| {
        Err(Error::new(
            ErrorKind::Unknown,
            "a queued command did not answer at once",
        ))
    })
}

/// A fresh `fred` pipeline on `client`, for one segment or one flush's settles.
pub(crate) fn pipeline_on(client: &Client) -> Pipeline<Client> {
    client.pipeline()
}
