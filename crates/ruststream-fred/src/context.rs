//! Typed per-delivery context exposing native Redis metadata, one struct per transport, plus the
//! subscription-scoped batch context the batch forms read.
//!
//! A handler reads native Redis metadata for the message it is processing by compile-time
//! [`Field`] key, with no hashing, boxing, or downcasting. The runtime builds the context value
//! once per delivery (via [`BuildContext`]) from the concrete broker message; the handler reads a
//! field with `ctx.context(key)`, or binds one as a parameter with the core `Ctx<K>` extractor.
//!
//! A batch handler gets one context per batch instead ([`BuildBatchContext`]), carrying only what
//! the whole *subscription* shares. Per-delivery data has no place there, since a batch spans many
//! deliveries: an entry id or a position rides the batch's own elements.
//!
//! This is purely additive. A handler that declares the default `()` context is unaffected: the
//! blanket `impl BuildContext<M> for ()` still applies, so opting in costs nothing to those who do
//! not.
//!
//! # What is exposed
//!
//! Only genuinely-native metadata that is not already reachable off the payload or
//! [`HeaderMap`](ruststream::HeaderMap) is surfaced here:
//!
//! * [`StreamContext`] (Redis Streams) - the entry id this delivery was read at, the position that
//!   redelivers it, the consumer group, and the group's reposition handle. The native reclaim
//!   delivery-count and idle time stay header-surfaced
//!   ([`DELIVERY_COUNT_HEADER`](crate::DELIVERY_COUNT_HEADER) /
//!   [`IDLE_MS_HEADER`](crate::IDLE_MS_HEADER)) and are deliberately not duplicated.
//! * [`StreamBatchContext`] (Redis Streams, batch forms) - the consumer group and its reposition
//!   handle, both subscription-scoped.
//! * [`PubSubContext`] (Redis Pub/Sub) - the concrete channel the message arrived on and whether it
//!   matched through a `PSUBSCRIBE` pattern (for a pattern subscription the channel differs from the
//!   registered glob).
//!
//! * [`PoolContext`] (every form) - the broker's connection pool alone, which
//!   [`keys::FredPool`] reads. The key also reads [`StreamContext`], [`StreamBatchContext`] and
//!   [`PubSubContext`], so a handler takes the pool beside a form's own keys; `Ctx<keys::FredPool>`
//!   goes after the form's key there, because the first `Ctx` key names the handler's context.
//!
//! Lists carry nothing native beyond their payload and headers, so a list handler reads the pool
//! off [`PoolContext`] or stays on the `()` default.
//!
//! # Examples
//!
//! ```
//! use ruststream::runtime::{Context, HandlerOutcome};
//! use ruststream_fred::context::{StreamContext, keys};
//!
//! // A handler over the Streams transport reading the native entry id and consumer group.
//! async fn handle(order: &Vec<u8>, ctx: &mut Context<'_, StreamContext>) -> HandlerOutcome {
//!     let id = ctx.context(keys::EntryId); // e.g. the stream entry id `1700000000000-0`
//!     println!("{} read through {}", id, ctx.context(keys::ConsumerGroup));
//!     HandlerOutcome::ack()
//! }
//! # let _ = handle;
//! ```

use std::convert::Infallible;

use fred::clients::Pool;
use ruststream::runtime::{Context, Ctx, FromContext};
use ruststream::{BuildBatchContext, BuildContext, Field};

use crate::list::RedisListMessage;
use crate::message::RedisMessage;
use crate::pipeline::{Form, RedisPipeline, RoundMessage};
use crate::pubsub::RedisPubSubMessage;
use crate::seek::{EntryId, RedisGroupPosition, RedisGroupSeeker};

/// Per-delivery context for a Redis Streams delivery ([`RedisMessage`]).
///
/// Built once per delivery from the message. Read its fields by [`keys`] key off a
/// [`ruststream::runtime::Context`], or bind one as a handler parameter with the core
/// `Ctx<K>` extractor. A body that repositions its group names this type as its context and needs
/// nothing else: the [`keys::SeekHandle`] key carries the live handle.
///
/// # Examples
///
/// ```
/// # mod demo {
/// use ruststream::prelude::*;
/// use ruststream::{Seeker, subscriber};
/// use ruststream_fred::context::keys;
/// use ruststream_fred::{RedisGroupPosition, RedisStream};
/// # #[derive(serde::Deserialize)]
/// # struct Order { id: u64 }
///
/// /// Skips the group past a region the producer marked poisoned.
/// #[subscriber(RedisStream::new("orders").group("workers"))]
/// async fn work(order: &Order, Ctx(seeker): Ctx<keys::SeekHandle>) -> HandlerOutcome {
///     if order.id == 0 && seeker.seek(RedisGroupPosition::end()).await.is_err() {
///         return HandlerOutcome::retry();
///     }
///     HandlerOutcome::ack()
/// }
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct StreamContext {
    entry: EntryId,
    position: RedisGroupPosition,
    // Carries the stream key and the consumer group too, so the group needs no second copy.
    seeker: RedisGroupSeeker,
}

impl StreamContext {
    /// The stream entry id (for example `1700000000000-0`) this delivery was read at.
    #[must_use]
    pub const fn entry_id(&self) -> EntryId {
        self.entry
    }

    /// The group cursor that redelivers this entry, as [`Positioned`](ruststream::Positioned)
    /// reports it: the cursor is exclusive, so it sits one id below the entry's own.
    #[must_use]
    pub const fn position(&self) -> RedisGroupPosition {
        self.position
    }

    /// The consumer group this delivery was read through.
    #[must_use]
    pub fn consumer_group(&self) -> &str {
        self.seeker.group()
    }

    /// The handle repositioning this delivery's consumer group.
    ///
    /// The cursor belongs to the group, so a seek through it moves every consumer of that group;
    /// see [`RedisGroupSeeker`].
    #[must_use]
    pub const fn seeker(&self) -> &RedisGroupSeeker {
        &self.seeker
    }

    /// The broker's connection pool, for a command whose answer the handler needs now.
    #[must_use]
    pub const fn pool(&self) -> &Pool {
        self.seeker.pool()
    }
}

impl BuildContext<RedisMessage> for StreamContext {
    fn build(msg: &RedisMessage) -> Self {
        Self {
            entry: msg.entry_id(),
            position: ruststream::Positioned::position(msg),
            // A clone of the subscription's pre-minted handle: reference-count bumps only,
            // nothing allocated per delivery.
            seeker: msg.seeker().clone(),
        }
    }
}

/// Batch context for a batched Redis Streams subscription: what the whole subscription shares.
///
/// The runtime builds one per dispatched batch from the batch's first delivery, and a batch body
/// reads it by [`keys`] key with `ctx.context(..)`. Per-delivery data (the entry id, the position
/// that redelivers one entry) has no place here, because a batch spans many deliveries: it rides
/// the batch's own elements instead, read off each element's typed header contract. Keeping this a
/// separate type from [`StreamContext`] is what rejects a batch body asking for per-delivery fields
/// at compile time.
///
/// # Examples
///
/// ```
/// # mod demo {
/// use ruststream::prelude::*;
/// use ruststream::{Seeker, subscriber};
/// use ruststream_fred::context::{StreamBatchContext, keys};
/// use ruststream_fred::{RedisGroupPosition, RedisStream};
/// # #[derive(serde::Deserialize)]
/// # struct Order { id: u64 }
///
/// /// A batch that saw the poison marker rewinds the group once the batch is settled.
/// #[subscriber(RedisStream::new("orders").group("workers"))]
/// async fn work(
///     batch: &[Order],
///     ctx: &mut Context<'_, StreamBatchContext>,
/// ) -> HandlerOutcome {
///     if batch.iter().any(|order| order.id == 0)
///         && ctx
///             .context(keys::SeekHandle)
///             .seek(RedisGroupPosition::beginning())
///             .await
///             .is_err()
///     {
///         return HandlerOutcome::retry();
///     }
///     HandlerOutcome::ack()
/// }
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct StreamBatchContext {
    seeker: RedisGroupSeeker,
}

impl StreamBatchContext {
    /// The consumer group every delivery of this batch was read through.
    #[must_use]
    pub fn consumer_group(&self) -> &str {
        self.seeker.group()
    }

    /// The handle repositioning this subscription's consumer group.
    #[must_use]
    pub const fn seeker(&self) -> &RedisGroupSeeker {
        &self.seeker
    }

    /// The broker's connection pool, for a command whose answer the batch needs now.
    #[must_use]
    pub const fn pool(&self) -> &Pool {
        self.seeker.pool()
    }
}

impl BuildBatchContext<RedisMessage> for StreamBatchContext {
    fn build(first: &RedisMessage) -> Self {
        Self {
            seeker: first.seeker().clone(),
        }
    }
}

/// Per-delivery context for a Redis Pub/Sub delivery ([`RedisPubSubMessage`]).
///
/// Pub/Sub keeps no history, so there is nothing to reposition and no position to report: the
/// fields are the delivery's own channel and how it matched.
#[derive(Debug, Clone)]
pub struct PubSubContext {
    channel: String,
    from_pattern: bool,
    pool: Pool,
}

impl PubSubContext {
    /// Constructs a context directly from its native fields (mainly for tests).
    ///
    /// # Examples
    ///
    /// ```
    /// use fred::clients::Pool;
    /// use fred::types::config::Config;
    /// use ruststream_fred::context::PubSubContext;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// // An unconnected pool: `Pool::new` opens no socket.
    /// let pool = Pool::new(Config::default(), None, None, None, 1)?;
    /// let cx = PubSubContext::new("events.eu", true, pool);
    /// assert_eq!(cx.channel(), "events.eu");
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn new(channel: impl Into<String>, from_pattern: bool, pool: Pool) -> Self {
        Self {
            channel: channel.into(),
            from_pattern,
            pool,
        }
    }

    /// The broker's connection pool, for a command whose answer the handler needs now.
    #[must_use]
    pub const fn pool(&self) -> &Pool {
        &self.pool
    }

    /// The concrete channel this message arrived on (the matched channel, not the subscription
    /// glob, for a pattern subscription).
    #[must_use]
    pub fn channel(&self) -> &str {
        &self.channel
    }

    /// Whether the delivery matched through a `PSUBSCRIBE` pattern rather than an exact subscribe.
    #[must_use]
    pub const fn from_pattern(&self) -> bool {
        self.from_pattern
    }
}

impl BuildContext<RedisPubSubMessage> for PubSubContext {
    fn build(msg: &RedisPubSubMessage) -> Self {
        Self {
            channel: msg.channel().to_owned(),
            from_pattern: msg.from_pattern(),
            pool: msg.pool().clone(),
        }
    }
}

/// The per-delivery context that carries the broker's connection pool and nothing else: what a
/// handler taking `Ctx<keys::FredPool>` alone reads, on every form.
///
/// # Examples
///
/// ```
/// # mod demo {
/// use fred::interfaces::KeysInterface;
/// use ruststream::prelude::*;
/// use ruststream::subscriber;
/// use ruststream_fred::RedisList;
/// use ruststream_fred::context::keys;
/// # #[derive(serde::Deserialize)]
/// # struct Job { id: u64 }
///
/// /// Counts every job in a key the handler reads back at once.
/// #[subscriber(RedisList::new("jobs").reliable())]
/// async fn work(job: &Job, Ctx(pool): Ctx<keys::FredPool>) -> HandlerOutcome {
///     let _ = job.id;
///     match pool.incr::<i64, _>("jobs.seen").await {
///         Ok(_) => HandlerOutcome::ack(),
///         Err(_) => HandlerOutcome::retry(),
///     }
/// }
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct PoolContext {
    pool: Pool,
}

impl PoolContext {
    /// The broker's connection pool.
    #[must_use]
    pub const fn pool(&self) -> &Pool {
        &self.pool
    }

    pub(crate) const fn from_pool(pool: Pool) -> Self {
        Self { pool }
    }
}

impl BuildContext<RedisMessage> for PoolContext {
    fn build(msg: &RedisMessage) -> Self {
        Self::from_pool(msg.seeker().pool().clone())
    }
}

impl BuildContext<RedisListMessage> for PoolContext {
    fn build(msg: &RedisListMessage) -> Self {
        Self::from_pool(msg.pool().clone())
    }
}

impl BuildContext<RedisPubSubMessage> for PoolContext {
    fn build(msg: &RedisPubSubMessage) -> Self {
        Self::from_pool(msg.pool().clone())
    }
}

/// The per-delivery context of a `.pipeline()` subscription: the delivery's round and the
/// broker's connection pool.
///
/// Only a pipelined subscription's delivery builds it, which is what makes `Ctx<keys::Pipeline>`
/// a compile error on a subscription without a window.
///
/// # Examples
///
/// ```
/// # mod demo {
/// use ruststream::prelude::*;
/// use ruststream::subscriber;
/// use ruststream_fred::PipelinedStream;
/// use ruststream_fred::context::{PipelineContext, keys};
/// # #[derive(serde::Deserialize)]
/// # struct Order { id: u64 }
///
/// #[subscriber(PipelinedStream::new("orders").group("workers"))]
/// async fn work(order: &Order, ctx: &mut Context<'_, PipelineContext>) -> HandlerOutcome {
///     let queued = ctx.context(keys::Pipeline).incr("orders.seen").await;
///     let _ = order.id;
///     if queued.is_err() {
///         return HandlerOutcome::retry();
///     }
///     HandlerOutcome::ack()
/// }
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct PipelineContext {
    pipeline: RedisPipeline,
    pool: Pool,
}

impl PipelineContext {
    /// The delivery's round: what the handler queues its commands into.
    #[must_use]
    pub const fn pipeline(&self) -> &RedisPipeline {
        &self.pipeline
    }

    /// The broker's connection pool, for a command whose answer the handler needs now.
    #[must_use]
    pub const fn pool(&self) -> &Pool {
        &self.pool
    }
}

impl<F: Form> BuildContext<RoundMessage<F>> for PipelineContext {
    fn build(msg: &RoundMessage<F>) -> Self {
        Self {
            pipeline: RedisPipeline::new(msg.round().clone()),
            pool: F::pool(msg.inner()).clone(),
        }
    }
}

/// A batch of a `.pipeline()` subscription is one segment: its context hands the batch body the
/// round every delivery of the batch shares.
impl<F: Form> BuildBatchContext<RoundMessage<F>> for PipelineContext {
    fn build(first: &RoundMessage<F>) -> Self {
        <Self as BuildContext<RoundMessage<F>>>::build(first)
    }
}

impl<F: Form> BuildContext<RoundMessage<F>> for PoolContext {
    fn build(msg: &RoundMessage<F>) -> Self {
        Self::from_pool(F::pool(msg.inner()).clone())
    }
}

/// `Ctx<keys::FredPool>` beside `Ctx<keys::Pipeline>`: the handler's context is
/// [`PipelineContext`] there.
impl<State: Sync> FromContext<PipelineContext, State> for Ctx<keys::FredPool> {
    type Rejection = Infallible;

    fn from_context(
        ctx: &mut Context<'_, PipelineContext, State>,
    ) -> impl Future<Output = Result<Self, Infallible>> + Send {
        let pool = ctx.context(keys::FredPool).clone();
        async move { Ok(Self(pool)) }
    }
}

/// `Ctx<keys::FredPool>` beside a stream key: the handler's context is [`StreamContext`] there.
impl<State: Sync> FromContext<StreamContext, State> for Ctx<keys::FredPool> {
    type Rejection = Infallible;

    fn from_context(
        ctx: &mut Context<'_, StreamContext, State>,
    ) -> impl Future<Output = Result<Self, Infallible>> + Send {
        let pool = ctx.context(keys::FredPool).clone();
        async move { Ok(Self(pool)) }
    }
}

/// `Ctx<keys::FredPool>` beside a Pub/Sub key: the handler's context is [`PubSubContext`] there.
impl<State: Sync> FromContext<PubSubContext, State> for Ctx<keys::FredPool> {
    type Rejection = Infallible;

    fn from_context(
        ctx: &mut Context<'_, PubSubContext, State>,
    ) -> impl Future<Output = Result<Self, Infallible>> + Send {
        let pool = ctx.context(keys::FredPool).clone();
        async move { Ok(Self(pool)) }
    }
}

/// Compile-time [`Field`] keys, one per native field, read with `ctx.context(key)`.
///
/// Each key is a zero-sized selector implementing [`Field`] only for the context types that carry
/// its field, so applying a key to the wrong transport's context is a compile error. A key that
/// also implements [`ContextField`](ruststream::ContextField) can be bound as a `Ctx<K>` handler
/// parameter; those read the per-delivery context, so a batch body reaches its own fields through
/// `ctx.context(..)` instead.
pub mod keys {
    use ruststream::ContextField;

    use fred::clients::Pool;

    use super::{
        Field, PipelineContext, PoolContext, PubSubContext, RedisGroupPosition, RedisGroupSeeker,
        RedisPipeline, StreamBatchContext, StreamContext,
    };

    /// Reads the delivery's round off the context of a `.pipeline()` subscription: the
    /// [`RedisPipeline`] its handler queues commands into.
    ///
    /// The key names [`PipelineContext`], which only a pipelined subscription's delivery builds,
    /// so taking it on a subscription without a window does not compile.
    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
    pub struct Pipeline;

    impl ContextField for Pipeline {
        type Context = PipelineContext;
        type Value = RedisPipeline;
        fn read(self, src: &PipelineContext) -> RedisPipeline {
            src.pipeline().clone()
        }
    }

    impl Field<PipelineContext> for Pipeline {
        type Value<'a> = &'a RedisPipeline;
        fn get(self, src: &PipelineContext) -> &RedisPipeline {
            src.pipeline()
        }
    }

    impl Field<PipelineContext> for FredPool {
        type Value<'a> = &'a Pool;
        fn get(self, src: &PipelineContext) -> &Pool {
            src.pool()
        }
    }

    /// Reads the broker's connection pool, `fred::clients::Pool`, off the context of any
    /// subscription.
    ///
    /// For a command whose answer the handler needs now, or one that must leave at once. As the
    /// only `Ctx` key of a handler it names [`PoolContext`]; beside a form's own key it reads
    /// that form's context, and goes after that key in the signature.
    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
    pub struct FredPool;

    impl ContextField for FredPool {
        type Context = PoolContext;
        type Value = Pool;
        fn read(self, src: &PoolContext) -> Pool {
            src.pool().clone()
        }
    }

    impl Field<PoolContext> for FredPool {
        type Value<'a> = &'a Pool;
        fn get(self, src: &PoolContext) -> &Pool {
            src.pool()
        }
    }

    impl Field<StreamContext> for FredPool {
        type Value<'a> = &'a Pool;
        fn get(self, src: &StreamContext) -> &Pool {
            src.pool()
        }
    }

    impl Field<StreamBatchContext> for FredPool {
        type Value<'a> = &'a Pool;
        fn get(self, src: &StreamBatchContext) -> &Pool {
            src.pool()
        }
    }

    impl Field<PubSubContext> for FredPool {
        type Value<'a> = &'a Pool;
        fn get(self, src: &PubSubContext) -> &Pool {
            src.pool()
        }
    }

    /// Reads the stream entry id this delivery was read at off a [`StreamContext`].
    ///
    /// The value is the parsed [`EntryId`](crate::EntryId), so it compares and orders the way the
    /// stream does; render it with `to_string` for the wire spelling.
    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
    pub struct EntryId;

    impl Field<StreamContext> for EntryId {
        type Value<'a> = crate::EntryId;
        fn get(self, src: &StreamContext) -> crate::EntryId {
            src.entry_id()
        }
    }

    impl ContextField for EntryId {
        type Context = StreamContext;
        type Value = crate::EntryId;
        fn read(self, src: &StreamContext) -> crate::EntryId {
            src.entry_id()
        }
    }

    /// Reads the group cursor that redelivers this entry off a [`StreamContext`].
    ///
    /// Seeking to it delivers this message again, followed by the entries after it.
    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
    pub struct Position;

    impl Field<StreamContext> for Position {
        type Value<'a> = RedisGroupPosition;
        fn get(self, src: &StreamContext) -> RedisGroupPosition {
            src.position()
        }
    }

    impl ContextField for Position {
        type Context = StreamContext;
        type Value = RedisGroupPosition;
        fn read(self, src: &StreamContext) -> RedisGroupPosition {
            src.position()
        }
    }

    /// Reads the group's reposition handle off a stream context, per delivery or per batch.
    ///
    /// The handle is subscription-scoped (resolved once, when the subscription opens), which is
    /// why it is the one field both context types carry. As a `Ctx<SeekHandle>` parameter it binds
    /// the per-delivery context; a batch body reads it with `ctx.context(SeekHandle)` off
    /// [`StreamBatchContext`].
    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
    pub struct SeekHandle;

    impl Field<StreamContext> for SeekHandle {
        type Value<'a> = &'a RedisGroupSeeker;
        fn get(self, src: &StreamContext) -> &RedisGroupSeeker {
            src.seeker()
        }
    }

    impl Field<StreamBatchContext> for SeekHandle {
        type Value<'a> = &'a RedisGroupSeeker;
        fn get(self, src: &StreamBatchContext) -> &RedisGroupSeeker {
            src.seeker()
        }
    }

    impl ContextField for SeekHandle {
        type Context = StreamContext;
        type Value = RedisGroupSeeker;
        fn read(self, src: &StreamContext) -> RedisGroupSeeker {
            src.seeker().clone()
        }
    }

    /// Reads the consumer group off a stream context, per delivery or per batch.
    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
    pub struct ConsumerGroup;

    impl Field<StreamContext> for ConsumerGroup {
        type Value<'a> = &'a str;
        fn get(self, src: &StreamContext) -> &str {
            src.consumer_group()
        }
    }

    impl Field<StreamBatchContext> for ConsumerGroup {
        type Value<'a> = &'a str;
        fn get(self, src: &StreamBatchContext) -> &str {
            src.consumer_group()
        }
    }

    impl ContextField for ConsumerGroup {
        type Context = StreamContext;
        type Value = String;
        fn read(self, src: &StreamContext) -> String {
            src.consumer_group().to_owned()
        }
    }

    /// Reads the concrete channel off a [`PubSubContext`].
    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
    pub struct Channel;

    impl Field<PubSubContext> for Channel {
        type Value<'a> = &'a str;
        fn get(self, src: &PubSubContext) -> &str {
            src.channel()
        }
    }

    impl ContextField for Channel {
        type Context = PubSubContext;
        type Value = String;
        fn read(self, src: &PubSubContext) -> String {
            src.channel().to_owned()
        }
    }

    /// Reads whether a [`PubSubContext`] delivery matched through a `PSUBSCRIBE` pattern.
    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
    pub struct FromPattern;

    impl Field<PubSubContext> for FromPattern {
        type Value<'a> = bool;
        fn get(self, src: &PubSubContext) -> bool {
            src.from_pattern()
        }
    }

    impl ContextField for FromPattern {
        type Context = PubSubContext;
        type Value = bool;
        fn read(self, src: &PubSubContext) -> bool {
            src.from_pattern()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::PubSubContext;
    use super::keys::{Channel, FromPattern};
    use fred::clients::Pool;
    use fred::types::config::Config;
    use ruststream::{ContextField, Field};

    /// An unconnected pool (just client structs); `Pool::new` opens no sockets.
    fn offline_pool() -> Pool {
        Pool::new(Config::default(), None, None, None, 1).expect("offline pool")
    }

    #[test]
    fn pubsub_keys_read_channel_and_pattern_flag() {
        let exact = PubSubContext::new("events", false, offline_pool());
        assert_eq!(Channel.get(&exact), "events");
        assert!(!FromPattern.get(&exact));

        let matched = PubSubContext::new("events.user", true, offline_pool());
        assert_eq!(Channel.get(&matched), "events.user");
        assert!(FromPattern.get(&matched));
    }

    #[test]
    fn pubsub_context_field_keys_yield_owned_values() {
        let pubsub = PubSubContext::new("orders.eu", true, offline_pool());
        assert_eq!(
            <Channel as ContextField>::read(Channel, &pubsub),
            "orders.eu".to_owned()
        );
        assert!(<FromPattern as ContextField>::read(FromPattern, &pubsub));
    }
}
