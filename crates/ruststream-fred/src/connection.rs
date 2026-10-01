//! The pooled connection a connected broker and every handle derived from it share, whether its
//! owner has shut it down, and the connection a subscription reads on.

use std::mem;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use fred::clients::{Client, Pool};
use fred::interfaces::{ClientInterface, ClientLike};
use fred::types::ClientUnblockFlag;
use ruststream::AckError;
use tokio::runtime::Handle;
use tokio::task::JoinHandle;

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

/// The connection one stream or list subscription reads on, apart from the pool.
///
/// A blocking read (`XREADGROUP ... BLOCK`, `BLMOVE`, `BRPOP`) holds its connection until it
/// returns, and `fred` queues every other command sent on that connection behind it. On a pooled
/// connection a publish that the pool hands the same client waits for the read's whole block
/// interval, so the reads go out on a connection of their own. The price is one connection per
/// subscription, opened when it subscribes.
///
/// Dropped, it interrupts a read still blocked on the server before it closes: a blocked pop left
/// running would take the next message for a subscription nobody reads any more. The close runs
/// on the broker's runtime, and the broker's shutdown waits for it.
pub(crate) struct ReadConnection {
    client: Client,
    /// The broker's runtime, where the close runs: a drop cannot await, and happens wherever the
    /// subscription is dropped.
    runtime: Handle,
    closing: Arc<Closing>,
    /// Whether a read can block on the server; the in-process one answers at once.
    blocks: bool,
}

impl ReadConnection {
    pub(crate) const fn new(
        client: Client,
        runtime: Handle,
        closing: Arc<Closing>,
        blocks: bool,
    ) -> Self {
        Self {
            client,
            runtime,
            closing,
            blocks,
        }
    }

    pub(crate) const fn client(&self) -> &Client {
        &self.client
    }
}

impl Drop for ReadConnection {
    fn drop(&mut self) {
        let client = self.client.clone();
        let blocks = self.blocks;
        let close = self.runtime.spawn(async move {
            if blocks {
                // `CLIENT UNBLOCK` goes out on `fred`'s side connection, so it overtakes the read
                // the `QUIT` below would queue behind. A connection that is not blocked answers
                // an error, which is the outcome wanted.
                let _ = client.unblock_self(Some(ClientUnblockFlag::Timeout)).await;
            }
            let _ = client.quit().await;
        });
        self.closing.track(close);
    }
}

/// The closes of read connections still running, which the broker's shutdown waits for.
#[derive(Debug, Default)]
pub(crate) struct Closing(Mutex<Vec<JoinHandle<()>>>);

impl Closing {
    fn track(&self, close: JoinHandle<()>) {
        let mut closes = self.0.lock().expect("read connection closes poisoned");
        closes.retain(|close| !close.is_finished());
        closes.push(close);
    }

    /// Waits for every close tracked so far.
    pub(crate) async fn finish(&self) {
        let closes = mem::take(&mut *self.0.lock().expect("read connection closes poisoned"));
        for close in closes {
            let _ = close.await;
        }
    }
}
