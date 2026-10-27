//! Publishes messages to Redis streams via `XADD`, with both transaction kinds on top: the
//! borrowed [`TransactionalPublisher`] on the handle and the owned [`OwnedTransactions`] value.

use std::fmt::{Debug, Formatter};
use std::future::{Future, ready};
use std::sync::{Arc, Mutex};

use bytes::BytesMut;
use fred::interfaces::{StreamsInterface, TransactionInterface};
use fred::types::Value;
#[cfg(feature = "asyncapi")]
use ruststream::asyncapi::Bindings;
use ruststream::{
    DefaultPublish, HeaderMap, OutgoingMessage, OwnedTransactions, PairError, PublishPolicy,
    Publisher, Take, Transaction, TransactionalPublisher,
};
use tracing::warn;

use crate::broker::{ConnectedRedisBroker, RedisCore};
use crate::envelope::{Envelope, EnvelopeChoice, Inherited, RustStream};
use crate::error::RedisError;
use crate::list::push;
use crate::partition::{RedisPublishOptions, resolved_headers};
use crate::pubsub::PubSubMode;
use crate::pubsub::send;
use crate::route::Route;

/// One buffered `XADD` (stream key plus its encoded entry fields), held while a transaction is open.
type Buffered = (String, Vec<(String, Vec<u8>)>);

/// Flushes `buffered` as one `MULTI` / `EXEC` block, in publish order.
///
/// The single flush path of both transaction kinds: they differ only in where the buffer lives
/// (the handle's slot for the borrowed kind, the [`RedisTransaction`] value for the owned one),
/// never in what a commit does.
///
/// # Errors
///
/// Returns [`RedisError::ShutDown`] when the connection is gone, or [`RedisError::Publish`] when
/// the block is rejected.
async fn flush_block(core: &RedisCore, buffered: Vec<Buffered>) -> Result<(), RedisError> {
    if buffered.is_empty() {
        return Ok(());
    }
    let pool = core.pool()?;
    let txn = pool.next().multi();
    for (key, fields) in buffered {
        // Queued client-side by `fred`; the whole block travels on one connection at `exec`.
        let _: () = txn
            .xadd(key, false, None::<()>, "*", fields)
            .await
            .map_err(RedisError::publish)?;
    }
    // `abort_on_error = true`: a command the server refuses to queue discards the block instead
    // of committing a partial one.
    let _: Value = txn.exec(true).await.map_err(RedisError::publish)?;
    Ok(())
}

/// The declaration half of the stream publisher: pure policy, constructible anywhere.
///
/// `XADD` needs no options beyond the target key, which travels on each message, and the envelope
/// the entry is written in, the broker's unless [`envelope`](Self::envelope) names another. The
/// policy in the broker's envelope is the value `RedisPublish`. It pairs into a [`RedisPublisher`]
/// against a [`ConnectedRedisBroker`], which is what makes "publishing before connect"
/// unrepresentable.
///
/// # Examples
///
/// ```
/// # mod demo {
/// use ruststream_fred::stream::prelude::*;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Deserialize)]
/// struct Order {
///     id: u64,
/// }
///
/// #[derive(Serialize, Outgoing)]
/// #[outgoing(name = "orders.accepted")]
/// struct Accepted {
///     id: u64,
/// }
///
/// #[subscriber(RedisStream::new("orders").group("workers"), reply)]
/// async fn accept(order: &Order) -> Accepted {
///     Accepted { id: order.id }
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
///         RedisBroker::standalone("redis://localhost:6379"),
///         |b| {
///             // `Publish` is this policy under the stream prelude's name. No connection exists
///             // yet; the runtime pairs it with the connected broker at startup.
///             b.include(accept).out_reply(Publish);
///         },
///     )
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[must_use]
pub struct RedisPublish<Env = Inherited> {
    envelope: Env,
}

/// The stream publish policy in the broker's envelope, the value a mount site names.
// The policy is a value in the value namespace beside the type of the same name, so a mount site
// writes `.out_reply(Publish)` as it did when the policy had no envelope to carry.
#[allow(
    non_upper_case_globals,
    reason = "the value of the type it is named after"
)]
pub const RedisPublish: RedisPublish = RedisPublish {
    envelope: Inherited,
};

impl<Env> RedisPublish<Env> {
    /// Writes the entries through `envelope` instead of the broker's: how a message's headers sit
    /// beside its payload in the entry's fields.
    ///
    /// # Examples
    ///
    /// ```
    /// # mod demo {
    /// use ruststream_fred::stream::prelude::*;
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(Deserialize)]
    /// struct Order {
    ///     id: u64,
    /// }
    ///
    /// #[derive(Serialize, Outgoing)]
    /// #[outgoing(name = "orders.accepted")]
    /// struct Accepted {
    ///     id: u64,
    /// }
    ///
    /// #[subscriber(RedisStream::new("orders").group("workers"), reply)]
    /// async fn accept(order: &Order) -> Accepted {
    ///     Accepted { id: order.id }
    /// }
    ///
    /// #[ruststream::app]
    /// fn app() -> impl App {
    ///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
    ///         RedisBroker::standalone("redis://localhost:6379"),
    ///         |b| {
    ///             // A faststream service reads `orders.accepted`.
    ///             b.include(accept).out_reply(Publish.envelope(FastStream));
    ///         },
    ///     )
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    pub fn envelope<Other: Envelope>(self, envelope: Other) -> RedisPublish<Other> {
        RedisPublish { envelope }
    }

    /// The envelope this policy names, resolved against the broker's when it pairs.
    pub(crate) fn into_envelope(self) -> Env {
        self.envelope
    }
}

impl<BrokerEnv, Env> PublishPolicy<ConnectedRedisBroker<BrokerEnv>> for RedisPublish<Env>
where
    BrokerEnv: Envelope,
    Env: EnvelopeChoice<BrokerEnv>,
{
    type Live = RedisPublisher<Env::Envelope>;

    fn pair(
        self,
        connected: &ConnectedRedisBroker<BrokerEnv>,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(connected.stream_publisher(self)))
    }

    /// An `XADD` carries its headers in the entry's fields, so what the document learns from here
    /// is which Redis structure the channel is, which stream key the entries are appended to, and
    /// the envelope they are written in.
    #[cfg(feature = "asyncapi")]
    fn channel_bindings(&self, channel: &str) -> Bindings {
        crate::asyncapi::channel(&crate::asyncapi::Publish::stream(
            channel,
            crate::asyncapi::Envelope::of::<Env::Envelope>(),
        ))
    }
}

/// The stream publisher's policy for a mount that publishes in transactions: it pairs into the same
/// [`RedisPublisher`] as [`RedisPublish`](struct@RedisPublish), and only where the topology can run one.
///
/// Redis Cluster cannot run a `MULTI` / `EXEC` block across hash slots, so on a cluster this
/// policy refuses to pair and the service fails to start, naming the registration that asked for
/// it. Without the refusal a `.transactional()` batch reply would fail to open its transaction on
/// every batch and redeliver it without end.
///
/// # Examples
///
/// ```
/// # mod demo {
/// use ruststream_fred::stream::prelude::*;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Deserialize, Serialize, Outgoing)]
/// #[outgoing(name = "orders.priced")]
/// struct Order {
///     id: u64,
/// }
///
/// #[subscriber(RedisStream::new("orders").group("workers"), reply)]
/// async fn price(orders: &[Order]) -> Result<Vec<Order>, HandlerOutcome> {
///     Ok(orders.iter().map(|order| Order { id: order.id }).collect())
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
///         RedisBroker::standalone("redis://localhost:6379"),
///         |b| {
///             // `TransactionalPublish` is this policy under the stream prelude's name: one
///             // batch's replies land together, as one MULTI / EXEC block.
///             b.include(price.batch(nonzero!(32)))
///                 .out_reply(TransactionalPublish)
///                 .transactional();
///         },
///     )
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[must_use]
pub struct RedisTransactionalPublish<Env = Inherited> {
    envelope: Env,
}

/// The transactional stream publish policy in the broker's envelope, the value a mount site
/// names.
// A value beside the type of the same name, as `RedisPublish` is.
#[allow(
    non_upper_case_globals,
    reason = "the value of the type it is named after"
)]
pub const RedisTransactionalPublish: RedisTransactionalPublish = RedisTransactionalPublish {
    envelope: Inherited,
};

impl<Env> RedisTransactionalPublish<Env> {
    /// Writes the entries through `envelope` instead of the broker's.
    ///
    /// # Examples
    ///
    /// ```
    /// # mod demo {
    /// use ruststream_fred::stream::prelude::*;
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(Deserialize, Serialize, Outgoing)]
    /// #[outgoing(name = "orders.priced")]
    /// struct Order {
    ///     id: u64,
    /// }
    ///
    /// #[subscriber(RedisStream::new("orders").group("workers"), reply)]
    /// async fn price(orders: &[Order]) -> Result<Vec<Order>, HandlerOutcome> {
    ///     Ok(orders.iter().map(|order| Order { id: order.id }).collect())
    /// }
    ///
    /// #[ruststream::app]
    /// fn app() -> impl App {
    ///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
    ///         RedisBroker::standalone("redis://localhost:6379"),
    ///         |b| {
    ///             // A faststream service reads `orders.priced`, one batch at a time.
    ///             b.include(price.batch(nonzero!(32)))
    ///                 .out_reply(TransactionalPublish.envelope(FastStream))
    ///                 .transactional();
    ///         },
    ///     )
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    pub fn envelope<Other: Envelope>(self, envelope: Other) -> RedisTransactionalPublish<Other> {
        RedisTransactionalPublish { envelope }
    }
}

impl<BrokerEnv, Env> PublishPolicy<ConnectedRedisBroker<BrokerEnv>>
    for RedisTransactionalPublish<Env>
where
    BrokerEnv: Envelope,
    Env: EnvelopeChoice<BrokerEnv>,
{
    type Live = RedisPublisher<Env::Envelope>;

    /// # Errors
    ///
    /// Returns a [`PairError`] carrying [`RedisError::InvalidOptions`] on a cluster topology.
    fn pair(
        self,
        connected: &ConnectedRedisBroker<BrokerEnv>,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        let publisher = connected.stream_publisher(RedisPublish {
            envelope: self.envelope,
        });
        ready(
            publisher
                .check_transactions_supported()
                .map(|()| publisher)
                .map_err(PairError::new),
        )
    }

    #[cfg(feature = "asyncapi")]
    fn channel_bindings(&self, channel: &str) -> Bindings {
        crate::asyncapi::channel(&crate::asyncapi::Publish::stream(
            channel,
            crate::asyncapi::Envelope::of::<Env::Envelope>(),
        ))
    }
}

/// The broker's default policy, what a registration publishes through when its mount site names
/// none: a reply, a retry copy, a dead-letter move.
///
/// It writes each name the way this service reads it. A name a [`RedisList`] subscription reads,
/// or dead-letters to, is `LPUSH`ed in that subscription's framing; a name a [`RedisPubSub`]
/// subscription reads or dead-letters to is published on in its mode and framing; any other name,
/// a stream key included, is `XADD`ed. So a retry copy and a dead-letter move leave through the
/// publish of the form the delivery came from, with nothing named at the mount site.
///
/// The route is recorded when a subscription opens, so a name this service does not subscribe to
/// is a stream. A pattern subscription records nothing: its mount site names both the destination
/// and the policy of its copies.
///
/// # Examples
///
/// ```
/// # mod demo {
/// use ruststream_fred::RedisList;
/// use ruststream_fred::stream::prelude::*;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Deserialize)]
/// struct Order {
///     id: u64,
/// }
///
/// #[derive(Deserialize, Serialize, Outgoing)]
/// #[outgoing(name = "jobs")]
/// struct Job {
///     order: u64,
/// }
///
/// #[subscriber(RedisStream::new("orders").group("planners"), reply)]
/// async fn plan(order: &Order) -> Job {
///     Job { order: order.id }
/// }
///
/// #[subscriber(RedisList::new("jobs").reliable())]
/// async fn work(job: &Job) -> HandlerOutcome {
///     tracing::info!(job.order, "job");
///     HandlerOutcome::ack()
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
///         RedisBroker::standalone("redis://localhost:6379"),
///         |b| {
///             // The reply names no policy, so it leaves through the default one: `jobs` is
///             // read as a list here, so the reply is an `LPUSH`.
///             b.include(plan);
///             b.include(work);
///         },
///     )
/// }
/// # }
/// # fn main() {}
/// ```
///
/// [`RedisList`]: crate::RedisList
/// [`RedisPubSub`]: crate::RedisPubSub
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[must_use]
pub struct RedisDefaultPublish;

impl<BrokerEnv: Envelope> PublishPolicy<ConnectedRedisBroker<BrokerEnv>> for RedisDefaultPublish {
    type Live = RedisDefaultPublisher<BrokerEnv>;

    fn pair(
        self,
        connected: &ConnectedRedisBroker<BrokerEnv>,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(connected.default_publisher()))
    }

    /// Described as the stream publish, the family of a name no subscription of the service reads;
    /// the document is built before any subscription opens, so a name that turns out to be read as
    /// a list or a channel is described as a stream.
    #[cfg(feature = "asyncapi")]
    fn channel_bindings(&self, channel: &str) -> Bindings {
        crate::asyncapi::channel(&crate::asyncapi::Publish::stream(
            channel,
            crate::asyncapi::Envelope::of::<BrokerEnv>(),
        ))
    }
}

impl<Env: Envelope> DefaultPublish for ConnectedRedisBroker<Env> {
    type Policy = RedisDefaultPublish;
}

/// The live form of [`RedisDefaultPublish`]: writes each name the way this connection's
/// subscriptions read it. Cheap to clone.
///
/// Obtain it from [`ConnectedRedisBroker::default_publisher`] or by pairing the policy. Like every
/// publisher here it may outlive the connection, so publishing after shutdown reports
/// [`RedisError::ShutDown`].
#[derive(Clone)]
pub struct RedisDefaultPublisher<Env = RustStream> {
    core: Arc<RedisCore>,
    /// What a name no subscription records an envelope for is written in: the broker's.
    envelope: Env,
    /// What `pipeline.bind(&out)` names this publisher and its clones by.
    round_name: u64,
}

impl<Env> Debug for RedisDefaultPublisher<Env> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisDefaultPublisher")
            .field("core", &self.core)
            .finish_non_exhaustive()
    }
}

impl<Env: Envelope> RedisDefaultPublisher<Env> {
    /// What `pipeline.bind(&out)` names this publisher and its clones by.
    pub(crate) const fn round_name(&self) -> u64 {
        self.round_name
    }

    pub(crate) fn new(core: Arc<RedisCore>, envelope: Env) -> Self {
        Self {
            round_name: core.rounds().publisher(),
            core,
            envelope,
        }
    }
}

impl<Env: Envelope> Publisher for RedisDefaultPublisher<Env> {
    /// As on [`RedisPublisher`]: a name nothing reads is a stream, whose `XADD` keeps the body.
    type Payload = Take;

    type Error = RedisError;
    /// The options every publisher of this crate takes, so a partition key resolves the same
    /// whichever family the name is written in.
    type Options = RedisPublishOptions;

    async fn publish(
        &self,
        msg: OutgoingMessage<'_, BytesMut>,
        options: Option<&Self::Options>,
    ) -> Result<(), Self::Error> {
        let (key, payload, headers) = msg.into_parts();
        let headers = resolved_headers(headers, options);
        let own = &self.envelope;
        let sealed = match self.core.routes().route(key) {
            Route::Stream { framing } => framing
                .seal_entry(own, key, Vec::from(payload), &headers)
                .map(Sealed::Entry),
            Route::List { framing } => framing.seal(own, key, &payload, &headers).map(Sealed::List),
            Route::Channel { mode, framing } => framing
                .seal(own, key, &payload, &headers)
                .map(|body| Sealed::Channel(mode, body)),
        }?;
        if let Some(round) = self
            .core
            .rounds()
            .joined(self.round_name, joins_round(options))
        {
            let joined = match sealed {
                Sealed::Entry(fields) => round.xadd(key, fields).await,
                Sealed::List(body) => round.lpush(key, body, None).await,
                Sealed::Channel(mode, body) => {
                    round.publish(key, body, mode == PubSubMode::Sharded).await
                }
            };
            return joined.map_err(RedisError::publish);
        }
        match sealed {
            Sealed::Entry(fields) => append(&self.core, key, fields).await,
            Sealed::List(body) => push(&self.core, None, key, body).await,
            Sealed::Channel(mode, body) => send(&self.core, mode, key, body).await,
        }
    }
}

/// A message the default publisher wrote in the envelope of the name's route, ready for the
/// command of its family.
enum Sealed {
    Entry(Vec<(String, Vec<u8>)>),
    List(Vec<u8>),
    Channel(PubSubMode, Vec<u8>),
}

/// Whether the call site asked this message to join the round of the delivery being handled.
pub(crate) fn joins_round(options: Option<&RedisPublishOptions>) -> bool {
    options.is_some_and(|options| options.join_round)
}

/// `XADD`s one entry onto the stream `key`: the stream publish, shared by [`RedisPublisher`] and
/// [`RedisDefaultPublisher`].
async fn append(
    core: &RedisCore,
    key: &str,
    fields: Vec<(String, Vec<u8>)>,
) -> Result<(), RedisError> {
    let pool = core.pool()?;
    let _: String = pool
        .xadd(key, false, None::<()>, "*", fields)
        .await
        .map_err(RedisError::publish)?;
    Ok(())
}

/// The live stream publisher: [`RedisPublish`](struct@RedisPublish) paired with a connection. Cheap to clone.
///
/// [`Publisher::publish`] appends the message to the stream named by
/// [`OutgoingMessage::name`](ruststream::OutgoingMessage::name) with `XADD <name> * ...`. The
/// payload and headers are encoded as entry fields (see [`crate::RedisStream`] for the consuming
/// side).
///
/// A publisher may outlive the connected broker it came from (it is a handle aliasing the
/// connection), so every operation after
/// [`shutdown`](ruststream::ConnectedBroker::shutdown) reports [`RedisError::ShutDown`] rather
/// than running against a closed pool.
///
/// # Transactions
///
/// Both framework transaction kinds are available on standalone and sentinel topologies, and both
/// commit the same way: the buffer is held client-side while the transaction is open and flushed
/// as one `MULTI` / `EXEC` block, in publish order, so subscribers see the whole batch or none of
/// it. They differ only in where that buffer lives.
///
/// * Borrowed ([`TransactionalPublisher`]): the handle carries one transaction.
///   [`begin_transaction`](TransactionalPublisher::begin_transaction) claims it and starts
///   buffering published messages, [`commit`](TransactionalPublisher::commit) flushes them, and
///   [`abort`](TransactionalPublisher::abort) discards them. Clones of a handle share the same
///   open transaction, and a second `begin_transaction` while one is open is rejected.
/// * Owned ([`OwnedTransactions`]): every [`transaction`](OwnedTransactions::transaction) call
///   returns a [`RedisTransaction`] owning its own buffer, so any number can be open on one
///   handle concurrently and the handle keeps publishing directly meanwhile.
///
/// Two Redis properties apply to both kinds. Cluster supports neither, because a `MULTI` block
/// cannot span hash slots, so opening a transaction there returns
/// [`RedisError::InvalidOptions`]. And Redis has no rollback: a command that fails at *runtime*
/// inside `EXEC` does not undo the commands before it. For a block of `XADD`s against stream keys
/// that is practically limited to out-of-memory and wrong-type keys; a command the server refuses
/// to *queue* discards the whole block.
///
/// # Examples
///
/// ```
/// # mod demo {
/// use ruststream::OutgoingMessage;
/// use ruststream_fred::stream::prelude::*;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Deserialize, Serialize, Outgoing)]
/// struct Order {
///     id: u64,
/// }
///
/// #[subscriber("orders", reply("processed"))]
/// async fn process(orders: &[Order]) -> Result<Vec<Order>, HandlerOutcome> {
///     Ok(orders.iter().map(|order| Order { id: order.id }).collect())
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     let broker = RedisBroker::standalone("redis://localhost:6379").default_group("workers");
///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
///         // The borrowed kind: one batch's replies, buffered and committed as one MULTI / EXEC.
///         b.include(process.batch(nonzero!(32)))
///             .out_reply(TransactionalPublish)
///             .transactional();
///
///         // The owned kind, in the hook where a service makes its first publishes.
///         b.after_startup(TransactionalPublish, async move |publisher| {
///             let mut seed = publisher.transaction().await?;
///             seed.publish(
///                 OutgoingMessage::new("processed", br#"{"id":0}"#.as_slice()),
///                 None,
///             )
///             .await?;
///             seed.commit().await
///         });
///     })
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Clone)]
pub struct RedisPublisher<Env = RustStream> {
    core: Arc<RedisCore>,
    /// What every entry is written in.
    envelope: Env,
    txn: Arc<Mutex<Option<Vec<Buffered>>>>,
    /// What `pipeline.bind(&out)` names this publisher and its clones by.
    round_name: u64,
}

impl<Env> Debug for RedisPublisher<Env> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisPublisher")
            .field("core", &self.core)
            .finish_non_exhaustive()
    }
}

impl<Env: Envelope> RedisPublisher<Env> {
    /// What `pipeline.bind(&out)` names this publisher and its clones by.
    pub(crate) const fn round_name(&self) -> u64 {
        self.round_name
    }

    pub(crate) fn new(core: Arc<RedisCore>, envelope: Env) -> Self {
        Self {
            round_name: core.rounds().publisher(),
            core,
            envelope,
            txn: Arc::new(Mutex::new(None)),
        }
    }

    /// The entry `payload` and `headers` make, for the stream `key`.
    fn entry(
        &self,
        key: &str,
        payload: BytesMut,
        headers: &HeaderMap,
    ) -> Result<Vec<(String, Vec<u8>)>, RedisError> {
        self.envelope
            .seal_entry(Vec::from(payload), headers)
            .map_err(|err| RedisError::seal(key, err))
    }

    /// Rejects a transaction on a topology that cannot offer one. Shared by both kinds so they
    /// answer identically.
    fn check_transactions_supported(&self) -> Result<(), RedisError> {
        if self.core.transactions_supported() {
            return Ok(());
        }
        Err(RedisError::InvalidOptions(
            "transactions are only supported on standalone and sentinel topologies".to_owned(),
        ))
    }

    /// Buffers `entry` if a transaction is open and returns `true`; otherwise leaves it for an
    /// immediate publish.
    fn buffer_if_in_txn(&self, entry: &Buffered) -> bool {
        let mut guard = self.txn.lock().expect("redis publisher mutex poisoned");
        let buffered = guard.as_mut().is_some_and(|buffer| {
            buffer.push(entry.clone());
            true
        });
        drop(guard);
        buffered
    }
}

impl<Env: Envelope> Publisher for RedisPublisher<Env> {
    /// `XADD` hands the body to the client as the value of a field, and the client keeps it, so
    /// the buffer the framework wrote becomes that value.
    type Payload = Take;

    type Error = RedisError;
    /// `XADD` takes the entry id and the trim threshold per command, and this publisher fixes
    /// both (`*` and no trim); the stream key is the message's name, not a setting. What is left
    /// to a call site is the partition key, which leaves as an entry field like any other header.
    type Options = RedisPublishOptions;

    async fn publish(
        &self,
        msg: OutgoingMessage<'_, BytesMut>,
        options: Option<&Self::Options>,
    ) -> Result<(), Self::Error> {
        if let Some(round) = self
            .core
            .rounds()
            .joined(self.round_name, joins_round(options))
        {
            let (key, payload, headers) = msg.into_parts();
            let fields = self.entry(key, payload, &resolved_headers(headers, options))?;
            return round.xadd(key, fields).await.map_err(RedisError::publish);
        }
        let (key, payload, headers) = msg.into_parts();
        let entry: Buffered = (
            key.to_owned(),
            self.entry(key, payload, &resolved_headers(headers, options))?,
        );
        if self.buffer_if_in_txn(&entry) {
            return Ok(());
        }
        let pool = self.core.pool()?;
        let (key, fields) = entry;
        let _: String = pool
            .xadd(key, false, None::<()>, "*", fields)
            .await
            .map_err(RedisError::publish)?;
        Ok(())
    }
}

impl<Env: Envelope> TransactionalPublisher for RedisPublisher<Env> {
    /// Starts buffering published messages on this handle.
    ///
    /// # Errors
    ///
    /// Returns [`RedisError::InvalidOptions`] on a cluster topology, which cannot offer
    /// multi-key transactions, or [`RedisError::TransactionBusy`] when a transaction is already
    /// open on this handle (the open one is left untouched).
    fn begin_transaction(&self) -> impl Future<Output = Result<(), Self::Error>> {
        if let Err(err) = self.check_transactions_supported() {
            return ready(Err(err));
        }
        {
            let mut guard = self.txn.lock().expect("redis publisher mutex poisoned");
            if guard.is_some() {
                return ready(Err(RedisError::TransactionBusy));
            }
            *guard = Some(Vec::new());
        }
        ready(Ok(()))
    }

    /// Flushes the buffered `XADD`s as one `MULTI` / `EXEC` block, in publish order, then clears
    /// the transaction.
    ///
    /// # Errors
    ///
    /// Returns [`RedisError::NoTransaction`] when no transaction is open on this handle,
    /// [`RedisError::ShutDown`] when the connection is gone, or [`RedisError::Publish`] if the
    /// block is rejected. On failure the transaction is already closed: the buffer is lost, and
    /// recovery is redelivery of the inputs rather than resubmission of the buffer.
    async fn commit(&self) -> Result<(), Self::Error> {
        // Taken before the flush: a failed commit has still closed the transaction.
        let buffered = self
            .txn
            .lock()
            .expect("redis publisher mutex poisoned")
            .take()
            .ok_or(RedisError::NoTransaction)?;
        flush_block(&self.core, buffered).await
    }

    /// Discards the buffered messages.
    ///
    /// # Errors
    ///
    /// Returns [`RedisError::NoTransaction`] when no transaction is open on this handle.
    fn abort(&self) -> impl Future<Output = Result<(), Self::Error>> {
        ready(
            self.txn
                .lock()
                .expect("redis publisher mutex poisoned")
                .take()
                .ok_or(RedisError::NoTransaction)
                .map(|_| ()),
        )
    }
}

/// Owned transactions: every [`transaction`](OwnedTransactions::transaction) call opens an
/// independent buffer-owning [`RedisTransaction`], so any number can be open concurrently on one
/// handle, next to (and unaffected by) the handle-level [`TransactionalPublisher`] transaction.
impl<Env: Envelope> OwnedTransactions for RedisPublisher<Env> {
    type Transaction = RedisTransaction<Env>;

    /// # Errors
    ///
    /// Returns [`RedisError::InvalidOptions`] on a cluster topology, which cannot offer
    /// multi-key transactions.
    fn transaction(&self) -> impl Future<Output = Result<RedisTransaction<Env>, RedisError>> {
        if let Err(err) = self.check_transactions_supported() {
            return ready(Err(err));
        }
        // Opening allocates a buffer and never touches the connection; a connection torn down
        // before the flush surfaces at commit, the visibility point, like the handle-level begin.
        ready(Ok(RedisTransaction {
            core: Arc::clone(&self.core),
            envelope: self.envelope.clone(),
            buffered: Vec::new(),
            settled: false,
        }))
    }
}

/// An owned Redis transaction, opened by [`transaction`](OwnedTransactions::transaction) on a
/// [`RedisPublisher`].
///
/// A private `XADD` buffer, flushed on commit through the same `MULTI` / `EXEC` path as the
/// handle-level kind (so the whole batch becomes visible atomically, in publish order) and
/// discarded on abort.
///
/// What sets it apart from the handle-level [`TransactionalPublisher`] buffer is ownership, not
/// the commit: any number of these can be open on one handle at a time, and the handle keeps
/// publishing directly while they are. The buffers are independent, so settling one never touches
/// another; only the flush itself takes a pooled connection.
///
/// # Examples
///
/// ```
/// # mod demo {
/// use ruststream::OutgoingMessage;
/// use ruststream_fred::stream::prelude::*;
/// # use serde::Deserialize;
/// # #[derive(Deserialize)]
/// # struct Order {
/// #     id: u64,
/// # }
/// #
/// # #[subscriber(RedisStream::new("orders").group("workers"))]
/// # async fn handle(order: &Order) -> HandlerOutcome {
/// #     tracing::info!(order.id, "order");
/// #     HandlerOutcome::ack()
/// # }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
///         RedisBroker::standalone("redis://localhost:6379"),
///         |b| {
///             b.include(handle);
///             // Two buffers open at once on one handle; each commits as its own MULTI / EXEC.
///             b.after_startup(TransactionalPublish, async move |publisher| {
///                 let mut orders = publisher.transaction().await?;
///                 let mut audit = publisher.transaction().await?;
///                 let seed = OutgoingMessage::new("orders", br#"{"id":0}"#.as_slice());
///                 orders.publish(seed, None).await?;
///                 let note = OutgoingMessage::new("audit", br#"{"seeded":1}"#.as_slice());
///                 audit.publish(note, None).await?;
///                 orders.commit().await?;
///                 audit.commit().await
///             });
///         },
///     )
/// }
/// # }
/// # fn main() {}
/// ```
#[must_use = "a transaction does nothing until settled with commit() or abort()"]
pub struct RedisTransaction<Env = RustStream> {
    core: Arc<RedisCore>,
    /// What every buffered entry is written in.
    envelope: Env,
    buffered: Vec<Buffered>,
    settled: bool,
}

impl<Env> Debug for RedisTransaction<Env> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisTransaction")
            .field("buffered", &self.buffered.len())
            .field("settled", &self.settled)
            .finish_non_exhaustive()
    }
}

impl<Env> Drop for RedisTransaction<Env> {
    fn drop(&mut self) {
        // Destructors cannot run async work, so a drop can only discard the buffer; the warning
        // marks that as an abort the caller never wrote.
        if !self.settled {
            warn!(
                target: "ruststream_fred",
                buffered = self.buffered.len(),
                "owned transaction dropped without commit or abort; its buffered messages are \
                 discarded"
            );
        }
    }
}

impl<Env: Envelope> Transaction for RedisTransaction<Env> {
    /// The same as [`RedisPublisher`]'s: a buffered `XADD` hands the client the same value.
    type Payload = Take;

    type Error = RedisError;
    /// The same as [`RedisPublisher`]'s: a buffered `XADD` is the same command, queued, so a
    /// staged message takes a partition key the way a direct one does.
    type Options = RedisPublishOptions;

    /// Buffers the `XADD` locally; nothing reaches the server before [`commit`](Self::commit).
    fn publish(
        &mut self,
        msg: OutgoingMessage<'_, BytesMut>,
        options: Option<&Self::Options>,
    ) -> impl Future<Output = Result<(), Self::Error>> {
        let (key, payload, headers) = msg.into_parts();
        let fields = self
            .envelope
            .seal_entry(Vec::from(payload), &resolved_headers(headers, options))
            .map_err(|err| RedisError::seal(key, err));
        ready(fields.map(|fields| self.buffered.push((key.to_owned(), fields))))
    }

    /// Flushes the buffer as one `MULTI` / `EXEC` block, in publish order.
    ///
    /// # Errors
    ///
    /// Returns [`RedisError::ShutDown`] when the connection this transaction was opened from is
    /// gone, or [`RedisError::Publish`] when the block is rejected. A failed commit has still
    /// consumed the transaction and its buffer is lost; redelivery of the inputs, not
    /// resubmission of the buffer, is the recovery path.
    async fn commit(mut self) -> Result<(), Self::Error> {
        // Settled before the flush: a failed commit has still consumed the transaction (the
        // buffer is lost per the Transaction contract), so the drop warning must not fire.
        self.settled = true;
        flush_block(&self.core, std::mem::take(&mut self.buffered)).await
    }

    /// Discards the buffer. Nothing was sent to the server, so this cannot fail.
    fn abort(mut self) -> impl Future<Output = Result<(), Self::Error>> {
        self.settled = true;
        ready(Ok(()))
    }
}
