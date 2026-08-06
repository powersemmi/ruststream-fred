//! Error type returned by Redis broker operations.

use std::error::Error as StdError;

use thiserror::Error;

/// Errors surfaced by the Redis broker implementation.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum RedisError {
    /// Failed to establish or use the underlying `fred` connection.
    #[error("redis connection error: {0}")]
    Connect(#[source] Box<dyn StdError + Send + Sync>),

    /// Failed to publish (`XADD`) a message to a stream.
    #[error("redis publish error: {0}")]
    Publish(#[source] Box<dyn StdError + Send + Sync>),

    /// Failed to open a subscription (consumer-group creation or the first read).
    #[error("redis subscribe error: {0}")]
    Subscribe(#[source] Box<dyn StdError + Send + Sync>),

    /// A stream read (`XREADGROUP` / `XAUTOCLAIM`) or acknowledgement (`XACK`) failed.
    #[error("redis stream error: {0}")]
    Stream(#[source] Box<dyn StdError + Send + Sync>),

    /// The operation ran through a handle aliasing a connection that was already shut down
    /// ([`ConnectedBroker::shutdown`](ruststream::ConnectedBroker::shutdown)).
    ///
    /// The lifecycle ladder makes misuse by the owner of the connected broker a compile error;
    /// publishers handed out before the shutdown outlive it, so their operations report this
    /// instead of running against a dead pool.
    #[error("the redis broker connection is shut down")]
    ShutDown,

    /// `begin_transaction` was called while a transaction is already open on this publisher handle.
    #[error("a transaction is already open on this publisher handle")]
    TransactionBusy,

    /// `commit` or `abort` was called with no open transaction on this publisher handle.
    #[error("no transaction is open on this publisher handle")]
    NoTransaction,

    /// The supplied subscription descriptor combines fields in a way the broker cannot honour
    /// (for example a [`RedisStream`](crate::RedisStream) with no consumer group, or a bare-string
    /// subscription with no broker-wide default group).
    #[error("invalid subscribe options: {0}")]
    InvalidOptions(String),
}

impl RedisError {
    /// Wraps a `fred` error as a [`RedisError::Stream`].
    pub(crate) fn stream(err: fred::error::Error) -> Self {
        Self::Stream(Box::new(err))
    }

    /// Wraps a `fred` error as a [`RedisError::Subscribe`].
    pub(crate) fn subscribe(err: fred::error::Error) -> Self {
        Self::Subscribe(Box::new(err))
    }

    /// Wraps a `fred` error as a [`RedisError::Publish`].
    pub(crate) fn publish(err: fred::error::Error) -> Self {
        Self::Publish(Box::new(err))
    }
}
