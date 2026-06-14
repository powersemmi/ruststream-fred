//! Publishes messages to Redis streams via `XADD`, with optional pipelined transactions.

use std::fmt::{Debug, Formatter};
use std::sync::{Arc, Mutex};

use fred::clients::Pool;
use fred::interfaces::StreamsInterface;
use ruststream::{OutgoingMessage, Publisher, TransactionalPublisher};
use tokio::sync::OnceCell;

use crate::{convert::fields_for_publish, error::RedisError};

/// One buffered `XADD` (stream key plus its encoded entry fields), held while a transaction is open.
type Buffered = (String, Vec<(String, Vec<u8>)>);

/// Redis publisher built on a shared `fred` connection pool. Cheap to clone.
///
/// Holds the broker's shared connection cell, so a publisher created before the broker connects
/// resolves the pool on first use; publishing before
/// [`Broker::connect`](ruststream::Broker::connect) returns [`RedisError::NotConnected`].
///
/// [`Publisher::publish`] appends the message to the stream named by
/// [`OutgoingMessage::name`](ruststream::OutgoingMessage::name) with `XADD <name> * ...`. The
/// payload and headers are encoded as entry fields (see [`crate::RedisStream`] for the consuming
/// side).
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
    pool: Arc<OnceCell<Pool>>,
    transactions_supported: bool,
    txn: Arc<Mutex<Option<Vec<Buffered>>>>,
}

impl Debug for RedisPublisher {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisPublisher")
            .field("transactions_supported", &self.transactions_supported)
            .finish_non_exhaustive()
    }
}

impl RedisPublisher {
    pub(crate) fn new(pool: Arc<OnceCell<Pool>>, transactions_supported: bool) -> Self {
        Self {
            pool,
            transactions_supported,
            txn: Arc::new(Mutex::new(None)),
        }
    }

    pub(crate) fn pool(&self) -> Result<Pool, RedisError> {
        self.pool.get().cloned().ok_or(RedisError::NotConnected)
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
        let pool = self.pool()?;
        let (key, fields) = entry;
        let _: String = pool
            .xadd(key, false, None::<()>, "*", fields)
            .await
            .map_err(RedisError::publish)?;
        Ok(())
    }
}

impl TransactionalPublisher for RedisPublisher {
    /// Starts buffering. A no-op if a transaction is already open (it continues).
    ///
    /// # Errors
    ///
    /// Returns [`RedisError::InvalidOptions`] on a cluster topology, which cannot offer
    /// multi-key transactions.
    async fn begin_transaction(&self) -> Result<(), Self::Error> {
        if !self.transactions_supported {
            return Err(RedisError::InvalidOptions(
                "transactions are only supported on standalone and sentinel topologies".to_owned(),
            ));
        }
        let mut guard = self.txn.lock().expect("redis publisher mutex poisoned");
        if guard.is_none() {
            *guard = Some(Vec::new());
        }
        drop(guard);
        Ok(())
    }

    /// Flushes the buffered `XADD`s in publish order through one pipeline, then clears the
    /// transaction. A commit with no open transaction is a no-op.
    ///
    /// # Errors
    ///
    /// Returns [`RedisError::NotConnected`] if the broker never connected, or
    /// [`RedisError::Publish`] if the pipeline fails. On failure the buffer is already cleared.
    async fn commit(&self) -> Result<(), Self::Error> {
        let buffered = self
            .txn
            .lock()
            .expect("redis publisher mutex poisoned")
            .take();
        let Some(buffered) = buffered else {
            return Ok(());
        };
        if buffered.is_empty() {
            return Ok(());
        }
        let pool = self.pool()?;
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

    /// Discards the buffered messages. An abort with no open transaction is a no-op.
    async fn abort(&self) -> Result<(), Self::Error> {
        self.txn
            .lock()
            .expect("redis publisher mutex poisoned")
            .take();
        Ok(())
    }
}
