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
use crate::envelope::frame;
use crate::list::push;
use crate::partition::{RedisPublishOptions, resolved_headers};
use crate::pubsub::PubSubMode;
use crate::pubsub::send;
use crate::route::Route;
use crate::{convert::fields_for_publish, error::RedisError};

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
/// `XADD` needs no options beyond the target key, which travels on each message, so the policy is
/// a unit marker. It pairs into a [`RedisPublisher`] against a [`ConnectedRedisBroker`], which is
/// what makes "publishing before connect" unrepresentable.
///
/// # Examples
///
/// ```no_run
/// use ruststream::{Broker, PublishPolicy};
/// use ruststream_fred::{RedisBroker, RedisPublish};
///
/// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
/// let policy = RedisPublish; // no connection in sight
/// let connected = RedisBroker::standalone("redis://localhost:6379").connect().await?;
/// let publisher = policy.pair(&connected).await?;
/// # let _ = publisher;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[must_use]
pub struct RedisPublish;

impl PublishPolicy<ConnectedRedisBroker> for RedisPublish {
    type Live = RedisPublisher;

    fn pair(
        self,
        connected: &ConnectedRedisBroker,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(connected.publisher()))
    }

    /// An `XADD` carries its headers as entry fields and the policy has no settings of its own, so
    /// what the document learns from here is which Redis structure the channel is and which stream
    /// key the entries are appended to.
    #[cfg(feature = "asyncapi")]
    fn channel_bindings(&self, channel: &str) -> Bindings {
        crate::asyncapi::channel(&crate::asyncapi::Publish::stream(channel))
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
/// ```no_run
/// use ruststream::{Broker, OutgoingMessage, PublishPolicy, Publisher};
/// use ruststream_fred::{RedisBroker, RedisDefaultPublish, RedisList};
///
/// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
/// let connected = RedisBroker::standalone("redis://localhost:6379").connect().await?;
/// let _jobs = connected.subscribe_list(RedisList::new("jobs").reliable()).await?;
/// let publisher = RedisDefaultPublish.pair(&connected).await?;
/// // `jobs` is read as a list here, so this is an `LPUSH`.
/// publisher.publish(OutgoingMessage::new("jobs", b"{}".as_slice()), None).await?;
/// # Ok(())
/// # }
/// ```
///
/// [`RedisList`]: crate::RedisList
/// [`RedisPubSub`]: crate::RedisPubSub
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[must_use]
pub struct RedisDefaultPublish;

impl PublishPolicy<ConnectedRedisBroker> for RedisDefaultPublish {
    type Live = RedisDefaultPublisher;

    fn pair(
        self,
        connected: &ConnectedRedisBroker,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(connected.default_publisher()))
    }

    /// Described as the stream publish, the family of a name no subscription of the service reads;
    /// the document is built before any subscription opens, so a name that turns out to be read as
    /// a list or a channel is described as a stream.
    #[cfg(feature = "asyncapi")]
    fn channel_bindings(&self, channel: &str) -> Bindings {
        crate::asyncapi::channel(&crate::asyncapi::Publish::stream(channel))
    }
}

impl DefaultPublish for ConnectedRedisBroker {
    type Policy = RedisDefaultPublish;
}

/// The live form of [`RedisDefaultPublish`]: writes each name the way this connection's
/// subscriptions read it. Cheap to clone.
///
/// Obtain it from [`ConnectedRedisBroker::default_publisher`] or by pairing the policy. Like every
/// publisher here it may outlive the connection, so publishing after shutdown reports
/// [`RedisError::ShutDown`].
#[derive(Clone)]
pub struct RedisDefaultPublisher {
    core: Arc<RedisCore>,
    /// What `pipeline.bind(&out)` names this publisher and its clones by.
    round_name: u64,
}

impl Debug for RedisDefaultPublisher {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisDefaultPublisher")
            .field("core", &self.core)
            .finish_non_exhaustive()
    }
}

impl RedisDefaultPublisher {
    /// What `pipeline.bind(&out)` names this publisher and its clones by.
    pub(crate) const fn round_name(&self) -> u64 {
        self.round_name
    }

    pub(crate) fn new(core: Arc<RedisCore>) -> Self {
        Self {
            round_name: core.rounds().publisher(),
            core,
        }
    }
}

impl Publisher for RedisDefaultPublisher {
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
        if let Some(round) = self
            .core
            .rounds()
            .joined(self.round_name, joins_round(options))
        {
            let joined = match self.core.routes().route(key) {
                Route::Stream => {
                    round
                        .xadd(key, fields_for_publish(Vec::from(payload), &headers))
                        .await
                }
                Route::List { envelope } => {
                    round
                        .lpush(key, frame(envelope.as_ref(), &payload, &headers), None)
                        .await
                }
                Route::Channel { mode, envelope } => {
                    round
                        .publish(
                            key,
                            frame(envelope.as_ref(), &payload, &headers),
                            mode == PubSubMode::Sharded,
                        )
                        .await
                }
            };
            return joined.map_err(RedisError::publish);
        }
        match self.core.routes().route(key) {
            Route::Stream => append(&self.core, key, Vec::from(payload), &headers).await,
            Route::List { envelope } => {
                push(&self.core, envelope.as_ref(), None, key, &payload, headers).await
            }
            Route::Channel { mode, envelope } => {
                send(&self.core, mode, envelope.as_ref(), key, &payload, headers).await
            }
        }
    }
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
    payload: Vec<u8>,
    headers: &HeaderMap,
) -> Result<(), RedisError> {
    let pool = core.pool()?;
    let _: String = pool
        .xadd(
            key,
            false,
            None::<()>,
            "*",
            fields_for_publish(payload, headers),
        )
        .await
        .map_err(RedisError::publish)?;
    Ok(())
}

/// Pairs the default policy against the in-process stand-in, which records the same routes the
/// real connection does, so a copy that leaves the wrong way on a server is refused here too.
#[cfg(feature = "testing")]
impl PublishPolicy<crate::testing::ConnectedRedisTestBroker> for RedisDefaultPublish {
    type Live = crate::testing::RedisTestPlainPublisher;

    fn pair(
        self,
        connected: &crate::testing::ConnectedRedisTestBroker,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(connected.default_publisher()))
    }

    /// The same body the real broker's policy writes, so a document built in a test is the
    /// document the service publishes.
    #[cfg(feature = "asyncapi")]
    fn channel_bindings(&self, channel: &str) -> Bindings {
        crate::asyncapi::channel(&crate::asyncapi::Publish::stream(channel))
    }
}

/// Pairs the production policy against the in-process stand-in, so a routes file's
/// `.out_reply(Publish)` mounts on both without naming a second type.
///
/// The policy carries nothing to honour (`XADD` takes its key from each message), and the
/// stand-in's publisher offers the same surface the live one does, both transaction kinds
/// included, so this form loses nothing in process.
#[cfg(feature = "testing")]
impl PublishPolicy<crate::testing::ConnectedRedisTestBroker> for RedisPublish {
    type Live = crate::testing::RedisTestPublisher;

    fn pair(
        self,
        connected: &crate::testing::ConnectedRedisTestBroker,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(connected.publisher()))
    }

    /// An `XADD` carries its headers as entry fields and the policy has no settings of its own, so
    /// what the document learns from here is which Redis structure the channel is and which stream
    /// key the entries are appended to.
    #[cfg(feature = "asyncapi")]
    fn channel_bindings(&self, channel: &str) -> Bindings {
        crate::asyncapi::channel(&crate::asyncapi::Publish::stream(channel))
    }
}

/// The live stream publisher: [`RedisPublish`] paired with a connection. Cheap to clone.
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
#[derive(Clone)]
pub struct RedisPublisher {
    core: Arc<RedisCore>,
    txn: Arc<Mutex<Option<Vec<Buffered>>>>,
    /// What `pipeline.bind(&out)` names this publisher and its clones by.
    round_name: u64,
}

impl Debug for RedisPublisher {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisPublisher")
            .field("core", &self.core)
            .finish_non_exhaustive()
    }
}

impl RedisPublisher {
    /// What `pipeline.bind(&out)` names this publisher and its clones by.
    pub(crate) const fn round_name(&self) -> u64 {
        self.round_name
    }

    pub(crate) fn new(core: Arc<RedisCore>) -> Self {
        Self {
            round_name: core.rounds().publisher(),
            core,
            txn: Arc::new(Mutex::new(None)),
        }
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

impl Publisher for RedisPublisher {
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
            let fields =
                fields_for_publish(Vec::from(payload), &resolved_headers(headers, options));
            return round.xadd(key, fields).await.map_err(RedisError::publish);
        }
        let (key, payload, headers) = msg.into_parts();
        let entry: Buffered = (
            key.to_owned(),
            fields_for_publish(Vec::from(payload), &resolved_headers(headers, options)),
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

impl TransactionalPublisher for RedisPublisher {
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
impl OwnedTransactions for RedisPublisher {
    type Transaction = RedisTransaction;

    /// # Errors
    ///
    /// Returns [`RedisError::InvalidOptions`] on a cluster topology, which cannot offer
    /// multi-key transactions.
    fn transaction(&self) -> impl Future<Output = Result<RedisTransaction, RedisError>> {
        if let Err(err) = self.check_transactions_supported() {
            return ready(Err(err));
        }
        // Opening allocates a buffer and never touches the connection; a connection torn down
        // before the flush surfaces at commit, the visibility point, like the handle-level begin.
        ready(Ok(RedisTransaction {
            core: Arc::clone(&self.core),
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
/// ```no_run
/// use ruststream::{Broker, OutgoingMessage, OwnedTransactions, Transaction};
/// use ruststream_fred::RedisBroker;
///
/// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
/// let connected = RedisBroker::standalone("redis://localhost:6379").connect().await?;
/// let publisher = connected.publisher();
///
/// let mut orders = publisher.transaction().await?;
/// let mut audit = publisher.transaction().await?; // concurrent with `orders`
/// orders.publish(OutgoingMessage::new("orders", b"{}".as_slice()), None).await?;
/// audit.publish(OutgoingMessage::new("audit", b"{}".as_slice()), None).await?;
/// orders.commit().await?;
/// audit.commit().await?;
/// # Ok(())
/// # }
/// ```
#[must_use = "a transaction does nothing until settled with commit() or abort()"]
pub struct RedisTransaction {
    core: Arc<RedisCore>,
    buffered: Vec<Buffered>,
    settled: bool,
}

impl Debug for RedisTransaction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisTransaction")
            .field("buffered", &self.buffered.len())
            .field("settled", &self.settled)
            .finish_non_exhaustive()
    }
}

impl Drop for RedisTransaction {
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

impl Transaction for RedisTransaction {
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
        self.buffered.push((
            key.to_owned(),
            fields_for_publish(Vec::from(payload), &resolved_headers(headers, options)),
        ));
        ready(Ok(()))
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
