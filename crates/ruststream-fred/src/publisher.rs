//! Publishes messages to Redis streams via `XADD`, with both transaction kinds on top: the
//! borrowed [`TransactionalPublisher`] on the handle and the owned [`OwnedTransactions`] value.

use std::fmt::{Debug, Formatter};
use std::sync::{Arc, Mutex};

use fred::interfaces::{StreamsInterface, TransactionInterface};
use fred::types::Value;
use ruststream::{
    DefaultPublish, OutgoingMessage, OwnedTransactions, PairError, PublishPolicy, Publisher,
    Transaction, TransactionalPublisher,
};
use tracing::warn;

use crate::broker::{ConnectedRedisBroker, RedisCore};
use crate::{convert::fields_for_publish, error::RedisError};

/// One buffered `XADD` (stream key plus its encoded entry fields), held while a transaction is open.
type Buffered = (String, Vec<(String, Vec<u8>)>);

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

    async fn pair(self, connected: &ConnectedRedisBroker) -> Result<Self::Live, PairError> {
        Ok(connected.publisher())
    }
}

impl DefaultPublish for ConnectedRedisBroker {
    type Policy = RedisPublish;
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
/// Both framework transaction kinds are available on standalone and sentinel topologies. Cluster
/// supports neither (buffered keys may live on different nodes), so opening one there returns
/// [`RedisError::InvalidOptions`].
///
/// * Borrowed ([`TransactionalPublisher`]): the handle carries one transaction.
///   [`begin_transaction`](TransactionalPublisher::begin_transaction) starts buffering published
///   messages, [`commit`](TransactionalPublisher::commit) flushes the buffer in publish order
///   through a single `fred` pipeline, and [`abort`](TransactionalPublisher::abort) discards it.
///   Clones of a handle share the same open transaction buffer, and a second
///   `begin_transaction` while one is open is rejected.
/// * Owned ([`OwnedTransactions`]): every [`transaction`](OwnedTransactions::transaction) call
///   returns a [`RedisTransaction`] owning its own buffer, so any number can be open on one
///   handle concurrently and the handle keeps publishing directly meanwhile. Its commit flushes
///   the buffer as one `MULTI` / `EXEC` block, which is where the two kinds differ in strength:
///   the borrowed pipeline batches the writes, the owned block also makes them atomic.
#[derive(Clone)]
pub struct RedisPublisher {
    core: Arc<RedisCore>,
    txn: Arc<Mutex<Option<Vec<Buffered>>>>,
}

impl Debug for RedisPublisher {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisPublisher")
            .field("core", &self.core)
            .finish_non_exhaustive()
    }
}

impl RedisPublisher {
    pub(crate) fn new(core: Arc<RedisCore>) -> Self {
        Self {
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
    type Error = RedisError;

    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        let entry: Buffered = (
            msg.name().to_owned(),
            fields_for_publish(msg.payload(), msg.headers()),
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
    async fn begin_transaction(&self) -> Result<(), Self::Error> {
        self.check_transactions_supported()?;
        let mut guard = self.txn.lock().expect("redis publisher mutex poisoned");
        if guard.is_some() {
            return Err(RedisError::TransactionBusy);
        }
        *guard = Some(Vec::new());
        drop(guard);
        Ok(())
    }

    /// Flushes the buffered `XADD`s in publish order through one pipeline, then clears the
    /// transaction.
    ///
    /// # Errors
    ///
    /// Returns [`RedisError::NoTransaction`] when no transaction is open on this handle,
    /// [`RedisError::ShutDown`] when the connection is gone, or [`RedisError::Publish`] if the
    /// pipeline fails. On failure the transaction is already closed: the buffer is lost, and
    /// recovery is redelivery of the inputs rather than resubmission of the buffer.
    async fn commit(&self) -> Result<(), Self::Error> {
        let buffered = self
            .txn
            .lock()
            .expect("redis publisher mutex poisoned")
            .take()
            .ok_or(RedisError::NoTransaction)?;
        if buffered.is_empty() {
            return Ok(());
        }
        let pool = self.core.pool()?;
        let pipeline = pool.next().pipeline();
        for (key, fields) in buffered {
            let _: () = pipeline
                .xadd(key, false, None::<()>, "*", fields)
                .await
                .map_err(RedisError::publish)?;
        }
        let _: Vec<Value> = pipeline.all().await.map_err(RedisError::publish)?;
        Ok(())
    }

    /// Discards the buffered messages.
    ///
    /// # Errors
    ///
    /// Returns [`RedisError::NoTransaction`] when no transaction is open on this handle.
    async fn abort(&self) -> Result<(), Self::Error> {
        self.txn
            .lock()
            .expect("redis publisher mutex poisoned")
            .take()
            .ok_or(RedisError::NoTransaction)
            .map(|_| ())
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
    async fn transaction(&self) -> Result<RedisTransaction, RedisError> {
        self.check_transactions_supported()?;
        // Opening allocates a buffer and never touches the connection; a connection torn down
        // before the flush surfaces at commit, the visibility point, like the handle-level begin.
        Ok(RedisTransaction {
            core: Arc::clone(&self.core),
            buffered: Vec::new(),
            settled: false,
        })
    }
}

/// An owned Redis transaction, opened by [`transaction`](OwnedTransactions::transaction) on a
/// [`RedisPublisher`].
///
/// A private `XADD` buffer, flushed to the server on commit as one `MULTI` / `EXEC` block (so the
/// whole batch becomes visible atomically, in publish order) and discarded on abort.
///
/// Unlike the handle-level [`TransactionalPublisher`] buffer, any number of these can be open on
/// one handle at a time, and the handle keeps publishing directly while they are. The buffers are
/// independent, so settling one never touches another; only the flush itself takes a pooled
/// connection.
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
/// orders.publish(OutgoingMessage::new("orders", b"{}".as_slice())).await?;
/// audit.publish(OutgoingMessage::new("audit", b"{}".as_slice())).await?;
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
    type Error = RedisError;

    /// Buffers the `XADD` locally; nothing reaches the server before [`commit`](Self::commit).
    async fn publish(&mut self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        self.buffered.push((
            msg.name().to_owned(),
            fields_for_publish(msg.payload(), msg.headers()),
        ));
        Ok(())
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
        if self.buffered.is_empty() {
            return Ok(());
        }
        let pool = self.core.pool()?;
        let txn = pool.next().multi();
        for (key, fields) in self.buffered.drain(..) {
            // Queued client-side by `fred`; the whole block travels on one connection at `exec`.
            let _: () = txn
                .xadd(key, false, None::<()>, "*", fields)
                .await
                .map_err(RedisError::publish)?;
        }
        // `abort_on_error = true`: a rejected queued command discards the block instead of
        // committing a partial one.
        let _: Value = txn.exec(true).await.map_err(RedisError::publish)?;
        Ok(())
    }

    /// Discards the buffer. Nothing was sent to the server, so this cannot fail.
    async fn abort(mut self) -> Result<(), Self::Error> {
        self.settled = true;
        Ok(())
    }
}
