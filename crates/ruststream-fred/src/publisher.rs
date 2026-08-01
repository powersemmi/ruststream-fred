//! Publishes messages to Redis streams via `XADD`, with optional pipelined transactions.

use std::fmt::{Debug, Formatter};
use std::sync::{Arc, Mutex};

use fred::interfaces::StreamsInterface;
use ruststream::{
    DefaultPublish, OutgoingMessage, PairError, PublishPolicy, Publisher, TransactionalPublisher,
};

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
/// On standalone and sentinel topologies the publisher implements [`TransactionalPublisher`]:
/// [`begin_transaction`](TransactionalPublisher::begin_transaction) starts buffering published
/// messages, [`commit`](TransactionalPublisher::commit) flushes the buffer in publish order through
/// a single `fred` pipeline, and [`abort`](TransactionalPublisher::abort) discards it. Cluster does
/// not support it (buffered keys may live on different nodes), so `begin_transaction` returns
/// [`RedisError::InvalidOptions`] there. Clones of a handle share the same open transaction buffer.
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
        if !self.core.transactions_supported() {
            return Err(RedisError::InvalidOptions(
                "transactions are only supported on standalone and sentinel topologies".to_owned(),
            ));
        }
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
        let _: Vec<fred::types::Value> = pipeline.all().await.map_err(RedisError::publish)?;
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
