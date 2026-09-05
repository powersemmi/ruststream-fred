//! [`RedisTestPublisher`]: `Publisher` plus both transaction kinds on top of the in-memory router,
//! and the [`RedisTestPublish`] policy it pairs from.

use std::future::{Future, ready};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use ruststream::{
    DefaultPublish, HeaderMap, OutgoingMessage, OwnedTransactions, PairError, PublishPolicy,
    Publisher, Transaction, TransactionalPublisher,
};
use tracing::warn;

use crate::{
    error::RedisError,
    testing::{
        ConnectedRedisTestBroker,
        broker::{TestBrokerState, validate_publish_key},
    },
};

/// One buffered publish (key, payload, headers), held while a transaction is open.
type Buffered = (String, Bytes, HeaderMap);

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

    fn pair(
        self,
        connected: &ConnectedRedisTestBroker,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(connected.publisher()))
    }
}

impl DefaultPublish for ConnectedRedisTestBroker {
    type Policy = RedisTestPublish;
}

/// Publisher returned by
/// [`ConnectedRedisTestBroker::publisher`](crate::testing::ConnectedRedisTestBroker::publisher).
///
/// Mirrors the real publisher's transaction surface, both kinds: messages published inside a
/// transaction are buffered and only fan out on commit (in publish order), or are discarded on
/// abort. The borrowed kind ([`TransactionalPublisher`]) uses the handle's single buffer; the
/// owned kind ([`OwnedTransactions`]) hands out an independent [`RedisTestTransaction`] per call.
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

    fn publish(&self, msg: OutgoingMessage<'_>) -> impl Future<Output = Result<(), Self::Error>> {
        if let Err(err) = validate_publish_key(msg.name()) {
            return ready(Err(err));
        }
        let entry: Buffered = (
            msg.name().to_owned(),
            Bytes::copy_from_slice(msg.payload()),
            msg.headers().clone(),
        );
        if self.buffer_if_in_txn(&entry) {
            return ready(Ok(()));
        }
        let (key, payload, headers) = entry;
        self.state
            .router
            .publish(key, payload, headers, self.state.coordinator().as_ref());
        ready(Ok(()))
    }
}

impl TransactionalPublisher for RedisTestPublisher {
    /// # Errors
    ///
    /// Returns [`RedisError::TransactionBusy`] when a transaction is already open on this handle,
    /// leaving that one untouched.
    fn begin_transaction(&self) -> impl Future<Output = Result<(), Self::Error>> {
        {
            let mut guard = self
                .txn
                .lock()
                .expect("redis test publisher mutex poisoned");
            if guard.is_some() {
                return ready(Err(RedisError::TransactionBusy));
            }
            *guard = Some(Vec::new());
        }
        ready(Ok(()))
    }

    /// # Errors
    ///
    /// Returns [`RedisError::NoTransaction`] when no transaction is open on this handle.
    fn commit(&self) -> impl Future<Output = Result<(), Self::Error>> {
        let buffered = self
            .txn
            .lock()
            .expect("redis test publisher mutex poisoned")
            .take();
        let Some(buffered) = buffered else {
            return ready(Err(RedisError::NoTransaction));
        };
        for (key, payload, headers) in buffered {
            self.state
                .router
                .publish(key, payload, headers, self.state.coordinator().as_ref());
        }
        ready(Ok(()))
    }

    /// # Errors
    ///
    /// Returns [`RedisError::NoTransaction`] when no transaction is open on this handle.
    fn abort(&self) -> impl Future<Output = Result<(), Self::Error>> {
        ready(
            self.txn
                .lock()
                .expect("redis test publisher mutex poisoned")
                .take()
                .ok_or(RedisError::NoTransaction)
                .map(|_| ()),
        )
    }
}

/// Owned transactions, mirroring [`RedisPublisher`](crate::RedisPublisher): every call opens an
/// independent buffer-owning [`RedisTestTransaction`], so any number can be open concurrently and
/// the handle keeps publishing directly meanwhile.
impl OwnedTransactions for RedisTestPublisher {
    type Transaction = RedisTestTransaction;

    fn transaction(&self) -> impl Future<Output = Result<RedisTestTransaction, RedisError>> {
        ready(Ok(RedisTestTransaction {
            state: Arc::clone(&self.state),
            buffered: Vec::new(),
            settled: false,
        }))
    }
}

/// An owned in-process transaction, opened by
/// [`transaction`](OwnedTransactions::transaction) on a [`RedisTestPublisher`].
///
/// A private buffer fanned out to the router in publish order on commit and discarded on abort,
/// standing in for the real publisher's `MULTI` / `EXEC` block.
///
/// # Examples
///
/// ```
/// use ruststream::{Broker, OutgoingMessage, OwnedTransactions, Transaction};
/// use ruststream_fred::testing::RedisTestBroker;
///
/// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
/// let connected = RedisTestBroker::new().connect().await?;
/// let publisher = connected.publisher();
/// let mut txn = publisher.transaction().await?;
/// txn.publish(OutgoingMessage::new("orders", b"{}".as_slice())).await?;
/// txn.commit().await?;
/// # Ok(())
/// # }
/// ```
#[must_use = "a transaction does nothing until settled with commit() or abort()"]
pub struct RedisTestTransaction {
    state: Arc<TestBrokerState>,
    buffered: Vec<Buffered>,
    settled: bool,
}

impl std::fmt::Debug for RedisTestTransaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisTestTransaction")
            .field("buffered", &self.buffered.len())
            .field("settled", &self.settled)
            .finish_non_exhaustive()
    }
}

impl Drop for RedisTestTransaction {
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

impl Transaction for RedisTestTransaction {
    type Error = RedisError;

    /// # Errors
    ///
    /// Returns [`RedisError::Publish`] when the stream key is empty, like the direct publish.
    fn publish(
        &mut self,
        msg: OutgoingMessage<'_>,
    ) -> impl Future<Output = Result<(), Self::Error>> {
        if let Err(err) = validate_publish_key(msg.name()) {
            return ready(Err(err));
        }
        self.buffered.push((
            msg.name().to_owned(),
            Bytes::copy_from_slice(msg.payload()),
            msg.headers().clone(),
        ));
        ready(Ok(()))
    }

    fn commit(mut self) -> impl Future<Output = Result<(), Self::Error>> {
        // Settled before the flush, as on the real publisher: a failed commit has still consumed
        // the transaction, so the drop warning must not fire.
        self.settled = true;
        for (key, payload, headers) in self.buffered.drain(..) {
            self.state
                .router
                .publish(key, payload, headers, self.state.coordinator().as_ref());
        }
        ready(Ok(()))
    }

    fn abort(mut self) -> impl Future<Output = Result<(), Self::Error>> {
        self.settled = true;
        ready(Ok(()))
    }
}
