//! The pooled connection a connected broker and every handle derived from it share, and whether
//! its owner has shut it down.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use fred::clients::Pool;
use ruststream::AckError;

use crate::error::RedisError;

/// The `fred` pool, with the shutdown flag beside it.
///
/// `fred` keeps a command sent to a client that has quit queued for a reconnect that never comes,
/// so a settlement or a seek issued through a delivery held past the shutdown would wait forever.
/// Every handle that outlives the connection reaches this, and asks it before it sends: after the
/// shutdown it gets [`RedisError::ShutDown`] at once. A subscription, a seeker and a list delivery
/// hold it behind one `Arc` in place of the pool; a stream delivery asks through the seeker it
/// already carries, so the check adds nothing to what a delivery holds.
pub(crate) struct Connection {
    pool: Pool,
    // Why a runtime flag: handles aliasing the connection outlive the consuming shutdown, which
    // the owner's typestate cannot reach.
    closed: AtomicBool,
}

impl Connection {
    pub(crate) fn new(pool: Pool) -> Arc<Self> {
        Arc::new(Self {
            pool,
            closed: AtomicBool::new(false),
        })
    }

    /// The pool to send a command on, or [`RedisError::ShutDown`] once the owner shut it down.
    pub(crate) fn live(&self) -> Result<&Pool, RedisError> {
        self.ensure_open()?;
        Ok(&self.pool)
    }

    /// [`RedisError::ShutDown`] once the owner shut the connection down.
    ///
    /// Asked before a command built on [`pool`](Self::pool), rather than through
    /// [`live`](Self::live) inside the command's expression, so the future the command returns is
    /// built and awaited exactly as it was without the check.
    #[inline]
    pub(crate) fn ensure_open(&self) -> Result<(), RedisError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(shut_down());
        }
        Ok(())
    }

    /// [`ensure_open`](Self::ensure_open) for a settlement, which reports the shutdown as a broker
    /// error.
    #[inline]
    pub(crate) fn ensure_open_to_settle(&self) -> Result<(), AckError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(AckError::Broker(Box::new(shut_down())));
        }
        Ok(())
    }

    /// The pool whatever the connection's state: what a handler is handed as its own client, and
    /// what the shutdown itself closes.
    pub(crate) const fn pool(&self) -> &Pool {
        &self.pool
    }

    /// Marks the connection shut down; every later [`live`](Self::live) refuses.
    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::Release);
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Relaxed)
    }
}

#[cold]
const fn shut_down() -> RedisError {
    RedisError::ShutDown
}
