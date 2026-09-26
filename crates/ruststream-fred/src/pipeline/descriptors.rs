//! The pipelined descriptors under names of their own, with their builder steps.
//!
//! `#[subscriber(..)]` reads a descriptor's type off the constructor its chain starts from, and
//! takes every step of the chain to keep that type. A chain that ends in `.pipeline()` changes it,
//! so the attribute spells a pipelined subscription from these constructors instead:
//! `PipelinedStream::new("orders").group("workers")` is
//! `RedisStream::new("orders").group("workers").pipeline()`, and `AtomicStream` is the same with
//! `.atomic()`.

use std::time::Duration;

use ruststream::codec::Codec;

use super::{Atomic, Pipelined, Plain, WindowMode};
use crate::delay::DelayedRetry;
use crate::pubsub::{PubSubMode, RedisPubSub};
use crate::{RedisList, RedisStream, StreamStart};

/// A stream subscription with a window: `RedisStream::new(..).pipeline()`, under a name
/// `#[subscriber(..)]` reads.
///
/// # Examples
///
/// ```
/// use ruststream_fred::PipelinedStream;
///
/// let orders = PipelinedStream::new("orders").group("workers");
/// assert_eq!(orders.descriptor().key(), "orders");
/// ```
pub type PipelinedStream = Pipelined<RedisStream, Plain>;

/// A stream subscription with an atomic window: `RedisStream::new(..).pipeline().atomic()`, under
/// a name `#[subscriber(..)]` reads.
///
/// # Examples
///
/// ```
/// use ruststream_fred::AtomicStream;
///
/// let orders = AtomicStream::new("{orders}").group("workers");
/// # let _ = orders;
/// ```
pub type AtomicStream = Pipelined<RedisStream, Atomic>;

/// A list subscription with a window: `RedisList::new(..).pipeline()`, under a name
/// `#[subscriber(..)]` reads.
///
/// # Examples
///
/// ```
/// use ruststream_fred::PipelinedList;
///
/// let jobs = PipelinedList::new("jobs").reliable();
/// # let _ = jobs;
/// ```
pub type PipelinedList = Pipelined<RedisList, Plain>;

/// A list subscription with an atomic window: `RedisList::new(..).pipeline().atomic()`, under a
/// name `#[subscriber(..)]` reads.
///
/// # Examples
///
/// ```
/// use ruststream_fred::AtomicList;
///
/// let jobs = AtomicList::new("{jobs}").reliable();
/// # let _ = jobs;
/// ```
pub type AtomicList = Pipelined<RedisList, Atomic>;

/// A Pub/Sub subscription with a window: `RedisPubSub::new(..).pipeline()`, under a name
/// `#[subscriber(..)]` reads.
///
/// # Examples
///
/// ```
/// use ruststream_fred::PipelinedPubSub;
///
/// let events = PipelinedPubSub::new("events");
/// # let _ = events;
/// ```
pub type PipelinedPubSub = Pipelined<RedisPubSub, Plain>;

/// A Pub/Sub subscription with an atomic window: `RedisPubSub::new(..).pipeline().atomic()`,
/// under a name `#[subscriber(..)]` reads.
///
/// # Examples
///
/// ```
/// use ruststream_fred::AtomicPubSub;
///
/// let events = AtomicPubSub::new("events");
/// # let _ = events;
/// ```
pub type AtomicPubSub = Pipelined<RedisPubSub, Atomic>;

impl<Descriptor, Mode> Pipelined<Descriptor, Mode> {
    fn map(self, step: impl FnOnce(Descriptor) -> Descriptor) -> Self {
        Self::wrap(step(self.descriptor))
    }
}

impl<Mode: WindowMode> Pipelined<RedisStream, Mode> {
    /// A fresh-tail subscription on `key`, as [`RedisStream::new`] opens it.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_fred::PipelinedStream;
    ///
    /// let orders = PipelinedStream::new("orders").group("workers");
    /// # let _ = orders;
    /// ```
    pub fn new(key: impl Into<String>) -> Self {
        Self::wrap(RedisStream::new(key))
    }

    /// A recovery subscription on `key`, as [`RedisStream::reclaim`] opens it.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use ruststream_fred::PipelinedStream;
    ///
    /// let stale = PipelinedStream::reclaim("orders", Duration::from_secs(30)).group("workers");
    /// # let _ = stale;
    /// ```
    pub fn reclaim(key: impl Into<String>, min_idle: Duration) -> Self {
        Self::wrap(RedisStream::reclaim(key, min_idle))
    }

    /// A claim-and-read subscription on `key`, as [`RedisStream::claiming`] opens it.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use ruststream_fred::PipelinedStream;
    ///
    /// let both = PipelinedStream::claiming("orders", Duration::from_secs(30)).group("workers");
    /// # let _ = both;
    /// ```
    pub fn claiming(key: impl Into<String>, min_idle: Duration) -> Self {
        Self::wrap(RedisStream::claiming(key, min_idle))
    }

    /// Sets the consumer group, as [`RedisStream::group`] does.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_fred::PipelinedStream;
    ///
    /// let orders = PipelinedStream::new("orders").group("workers");
    /// # let _ = orders;
    /// ```
    pub fn group(self, group: impl Into<String>) -> Self {
        self.map(|stream| stream.group(group))
    }

    /// Sets this consumer's name, as [`RedisStream::consumer`] does.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_fred::PipelinedStream;
    ///
    /// let orders = PipelinedStream::new("orders").group("workers").consumer("worker-1");
    /// # let _ = orders;
    /// ```
    pub fn consumer(self, consumer: impl Into<String>) -> Self {
        self.map(|stream| stream.consumer(consumer))
    }

    /// Sets how long one read blocks, as [`RedisStream::block`] does.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use ruststream_fred::PipelinedStream;
    ///
    /// let orders = PipelinedStream::new("orders").group("workers").block(Duration::from_secs(1));
    /// # let _ = orders;
    /// ```
    pub fn block(self, block: Duration) -> Self {
        self.map(|stream| stream.block(block))
    }

    /// Sets where a newly created group starts reading, as [`RedisStream::start_id`] does.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_fred::{PipelinedStream, StreamStart};
    ///
    /// let orders = PipelinedStream::new("orders")
    ///     .group("workers")
    ///     .start_id(StreamStart::Beginning);
    /// # let _ = orders;
    /// ```
    pub fn start_id(self, start: StreamStart) -> Self {
        self.map(|stream| stream.start_id(start))
    }

    /// Names a ZSET delay queue, as [`RedisStream::delayed_retry`] does.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_fred::{DelayedRetry, PipelinedStream};
    ///
    /// let orders = PipelinedStream::new("orders").group("workers").delayed_retry(
    ///     DelayedRetry::DurableZset { key: "orders.delayed".to_owned(), ttl: None },
    /// );
    /// # let _ = orders;
    /// ```
    pub fn delayed_retry(self, retry: DelayedRetry) -> Self {
        self.map(|stream| stream.delayed_retry(retry))
    }
}

impl<Mode: WindowMode> Pipelined<RedisList, Mode> {
    /// A list subscription on `key`, as [`RedisList::new`] opens it.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_fred::PipelinedList;
    ///
    /// let jobs = PipelinedList::new("jobs");
    /// # let _ = jobs;
    /// ```
    pub fn new(key: impl Into<String>) -> Self {
        Self::wrap(RedisList::new(key))
    }

    /// Switches to reliable mode, as [`RedisList::reliable`] does.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_fred::PipelinedList;
    ///
    /// let jobs = PipelinedList::new("jobs").reliable();
    /// # let _ = jobs;
    /// ```
    pub fn reliable(self) -> Self {
        self.map(RedisList::reliable)
    }

    /// Sets the processing list, as [`RedisList::processing`] does.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_fred::PipelinedList;
    ///
    /// let jobs = PipelinedList::new("jobs").reliable().processing("jobs.claimed");
    /// # let _ = jobs;
    /// ```
    pub fn processing(self, key: impl Into<String>) -> Self {
        self.map(|list| list.processing(key))
    }

    /// Sets how long a pop that waits blocks, as [`RedisList::block`] does.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use ruststream_fred::PipelinedList;
    ///
    /// let jobs = PipelinedList::new("jobs").block(Duration::from_secs(1));
    /// # let _ = jobs;
    /// ```
    pub fn block(self, block: Duration) -> Self {
        self.map(|list| list.block(block))
    }

    /// Decodes the envelope with `codec`, as [`RedisList::codec`] does.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::codec::JsonCodec;
    /// use ruststream_fred::PipelinedList;
    ///
    /// let jobs = PipelinedList::new("jobs").codec(JsonCodec);
    /// # let _ = jobs;
    /// ```
    pub fn codec(self, codec: impl Codec + 'static) -> Self {
        self.map(|list| list.codec(codec))
    }

    /// Sets the recovery watchdog's idle threshold, as [`RedisList::min_idle`] does.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use ruststream_fred::PipelinedList;
    ///
    /// let jobs = PipelinedList::new("jobs")
    ///     .recovery_zset("jobs.claims")
    ///     .min_idle(Duration::from_secs(60));
    /// # let _ = jobs;
    /// ```
    pub fn min_idle(self, min_idle: Duration) -> Self {
        self.map(|list| list.min_idle(min_idle))
    }

    /// Opts into orphan recovery, as [`RedisList::recovery_zset`] does.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use ruststream_fred::PipelinedList;
    ///
    /// let jobs = PipelinedList::new("jobs")
    ///     .recovery_zset("jobs.claims")
    ///     .min_idle(Duration::from_secs(60));
    /// # let _ = jobs;
    /// ```
    pub fn recovery_zset(self, key: impl Into<String>) -> Self {
        self.map(|list| list.recovery_zset(key))
    }

    /// Sets the recovery ZSET's expiry, as [`RedisList::recovery_ttl`] does.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use ruststream_fred::PipelinedList;
    ///
    /// let jobs = PipelinedList::new("jobs")
    ///     .recovery_zset("jobs.claims")
    ///     .min_idle(Duration::from_secs(60))
    ///     .recovery_ttl(Duration::from_secs(3600));
    /// # let _ = jobs;
    /// ```
    pub fn recovery_ttl(self, ttl: Duration) -> Self {
        self.map(|list| list.recovery_ttl(ttl))
    }
}

impl<Mode: WindowMode> Pipelined<RedisPubSub, Mode> {
    /// A subscription on the exact `channel`, as [`RedisPubSub::new`] opens it.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_fred::PipelinedPubSub;
    ///
    /// let events = PipelinedPubSub::new("events");
    /// # let _ = events;
    /// ```
    pub fn new(channel: impl Into<String>) -> Self {
        Self::wrap(RedisPubSub::new(channel))
    }

    /// Sets the delivery mode, as [`RedisPubSub::mode`] does.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_fred::{PipelinedPubSub, PubSubMode};
    ///
    /// let events = PipelinedPubSub::new("events").mode(PubSubMode::Sharded);
    /// # let _ = events;
    /// ```
    pub fn mode(self, mode: PubSubMode) -> Self {
        self.map(|pubsub| pubsub.mode(mode))
    }

    /// Decodes the envelope with `codec`, as [`RedisPubSub::codec`] does.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::codec::JsonCodec;
    /// use ruststream_fred::PipelinedPubSub;
    ///
    /// let events = PipelinedPubSub::new("events").codec(JsonCodec);
    /// # let _ = events;
    /// ```
    pub fn codec(self, codec: impl Codec + 'static) -> Self {
        self.map(|pubsub| pubsub.codec(codec))
    }
}
