//! [`RedisTestPublisher`]: `Publisher` + `TransactionalPublisher` on top of the in-memory router.

use std::sync::{Arc, Mutex};

use bytes::Bytes;
use ruststream::{Headers, OutgoingMessage, Publisher, TransactionalPublisher};

use crate::{
    error::RedisError,
    testing::broker::{TestBrokerState, validate_publish_key},
};

/// One buffered publish (key, payload, headers), held while a transaction is open.
type Buffered = (String, Bytes, Headers);

/// Publisher returned by [`crate::testing::RedisTestBroker::publisher`].
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
    async fn begin_transaction(&self) -> Result<(), Self::Error> {
        let mut guard = self
            .txn
            .lock()
            .expect("redis test publisher mutex poisoned");
        if guard.is_none() {
            *guard = Some(Vec::new());
        }
        drop(guard);
        Ok(())
    }

    async fn commit(&self) -> Result<(), Self::Error> {
        let buffered = self
            .txn
            .lock()
            .expect("redis test publisher mutex poisoned")
            .take();
        for (key, payload, headers) in buffered.into_iter().flatten() {
            self.state
                .router
                .publish(key, payload, headers, self.state.coordinator().as_ref());
        }
        Ok(())
    }

    async fn abort(&self) -> Result<(), Self::Error> {
        self.txn
            .lock()
            .expect("redis test publisher mutex poisoned")
            .take();
        Ok(())
    }
}
