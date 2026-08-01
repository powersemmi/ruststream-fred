//! [`RedisTestPublisher`]: `Publisher` + `TransactionalPublisher` on top of the in-memory router,
//! and the [`RedisTestPublish`] policy it pairs from.

use std::sync::{Arc, Mutex};

use bytes::Bytes;
use ruststream::{
    DefaultPublish, Headers, OutgoingMessage, PairError, PublishPolicy, Publisher,
    TransactionalPublisher,
};

use crate::{
    error::RedisError,
    testing::{
        ConnectedRedisTestBroker,
        broker::{TestBrokerState, validate_publish_key},
    },
};

/// One buffered publish (key, payload, headers), held while a transaction is open.
type Buffered = (String, Bytes, Headers);

/// The publish policy of the in-process broker, mirroring [`RedisPublish`](crate::RedisPublish).
///
/// # Examples
///
/// ```
/// use ruststream_fred::testing::RedisTestPublish;
///
/// let policy = RedisTestPublish::default();
/// # let _ = policy;
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[must_use]
pub struct RedisTestPublish;

impl PublishPolicy<ConnectedRedisTestBroker> for RedisTestPublish {
    type Live = RedisTestPublisher;

    async fn pair(self, connected: &ConnectedRedisTestBroker) -> Result<Self::Live, PairError> {
        Ok(connected.publisher())
    }
}

impl DefaultPublish for ConnectedRedisTestBroker {
    type Policy = RedisTestPublish;
}

/// Publisher returned by
/// [`ConnectedRedisTestBroker::publisher`](crate::testing::ConnectedRedisTestBroker::publisher).
///
/// Mirrors the real publisher's transaction surface: messages published inside a transaction are
/// buffered and only fan out on [`commit`](TransactionalPublisher::commit) (in publish order), or
/// are discarded on [`abort`](TransactionalPublisher::abort).
#[derive(Clone)]
pub struct RedisTestPublisher {
    state: Arc<TestBrokerState>,
    txn: Arc<Mutex<Option<Vec<Buffered>>>>,
}

impl std::fmt::Debug for RedisTestPublisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisTestPublisher").finish_non_exhaustive()
    }
}

impl RedisTestPublisher {
    pub(crate) fn new(state: Arc<TestBrokerState>) -> Self {
        Self {
            state,
            txn: Arc::new(Mutex::new(None)),
        }
    }

    fn buffer_if_in_txn(&self, entry: &Buffered) -> bool {
        let mut guard = self
            .txn
            .lock()
            .expect("redis test publisher mutex poisoned");
        let buffered = guard.as_mut().is_some_and(|buffer| {
            buffer.push(entry.clone());
            true
        });
        drop(guard);
        buffered
    }
}

impl Publisher for RedisTestPublisher {
    type Error = RedisError;

    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        validate_publish_key(msg.name())?;
        let entry: Buffered = (
            msg.name().to_owned(),
            Bytes::copy_from_slice(msg.payload()),
            msg.headers().clone(),
        );
        if self.buffer_if_in_txn(&entry) {
            return Ok(());
        }
        let (key, payload, headers) = entry;
        self.state
            .router
            .publish(key, payload, headers, self.state.coordinator().as_ref());
        Ok(())
    }
}

impl TransactionalPublisher for RedisTestPublisher {
    /// # Errors
    ///
    /// Returns [`RedisError::TransactionBusy`] when a transaction is already open on this handle,
    /// leaving that one untouched.
    async fn begin_transaction(&self) -> Result<(), Self::Error> {
        let mut guard = self
            .txn
            .lock()
            .expect("redis test publisher mutex poisoned");
        if guard.is_some() {
            return Err(RedisError::TransactionBusy);
        }
        *guard = Some(Vec::new());
        drop(guard);
        Ok(())
    }

    /// # Errors
    ///
    /// Returns [`RedisError::NoTransaction`] when no transaction is open on this handle.
    async fn commit(&self) -> Result<(), Self::Error> {
        let buffered = self
            .txn
            .lock()
            .expect("redis test publisher mutex poisoned")
            .take()
            .ok_or(RedisError::NoTransaction)?;
        for (key, payload, headers) in buffered {
            self.state
                .router
                .publish(key, payload, headers, self.state.coordinator().as_ref());
        }
        Ok(())
    }

    /// # Errors
    ///
    /// Returns [`RedisError::NoTransaction`] when no transaction is open on this handle.
    async fn abort(&self) -> Result<(), Self::Error> {
        self.txn
            .lock()
            .expect("redis test publisher mutex poisoned")
            .take()
            .ok_or(RedisError::NoTransaction)
            .map(|_| ())
    }
}
