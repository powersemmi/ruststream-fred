//! Typed per-delivery context exposing native Redis metadata, one struct per transport, plus the
//! subscription-scoped page context the batch forms read.
//!
//! A handler reads native Redis metadata for the message it is processing by compile-time
//! [`Field`] key, with no hashing, boxing, or downcasting. The runtime builds the context value
//! once per delivery (via [`BuildContext`]) from the concrete broker message; the handler reads a
//! field with `ctx.context(key)`, or binds one as a parameter with the core `Ctx<K>` extractor.
//!
//! A batch handler gets one context per page instead ([`BuildBatchContext`]), carrying only what
//! the whole *subscription* shares. Per-delivery data has no place there, since a page spans many
//! deliveries: an entry id or a position rides the page's own elements.
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
//! Lists carry nothing native beyond their payload and headers, so they stay on the `()` default.
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

use ruststream::{BuildBatchContext, BuildContext, Field};

use crate::message::RedisMessage;
use crate::pubsub::RedisPubSubMessage;
use crate::seek::{EntryId, RedisGroupPosition, RedisGroupSeeker};

/// Per-delivery context for a Redis Streams delivery ([`RedisMessage`]).
///
/// Built once per delivery from the message. Read its fields by [`keys`] key off a
/// [`Context`](ruststream::runtime::Context), or bind one as a handler parameter with the core
/// `Ctx<K>` extractor. A body that repositions its group names this type as its context and needs
/// nothing else: the [`keys::SeekHandle`] key carries the live handle.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "macros")]
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

/// Page context for a batched Redis Streams subscription: what the whole subscription shares.
///
/// The runtime builds one per dispatched page from the page's first delivery, and a page body
/// reads it by [`keys`] key with `ctx.context(..)`. Per-delivery data (the entry id, the position
/// that redelivers one entry) has no place here, because a page spans many deliveries: it rides
/// the page's own elements instead, read off each element's typed header contract. Keeping this a
/// separate type from [`StreamContext`] is what rejects a page body asking for per-delivery fields
/// at compile time.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "macros")]
/// # mod demo {
/// use ruststream::prelude::*;
/// use ruststream::{Seeker, subscriber};
/// use ruststream_fred::context::{StreamBatchContext, keys};
/// use ruststream_fred::{RedisGroupPosition, RedisStream};
/// # #[derive(serde::Deserialize)]
/// # struct Order { id: u64 }
///
/// /// A page that saw the poison marker rewinds the group once the page is settled.
/// #[subscriber(RedisStream::new("orders").group("workers"))]
/// async fn work(
///     page: &[Order],
///     ctx: &mut Context<'_, StreamBatchContext>,
/// ) -> HandlerOutcome {
///     if page.iter().any(|order| order.id == 0)
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
    /// The consumer group every delivery of this page was read through.
    #[must_use]
    pub fn consumer_group(&self) -> &str {
        self.seeker.group()
    }

    /// The handle repositioning this subscription's consumer group.
    #[must_use]
    pub const fn seeker(&self) -> &RedisGroupSeeker {
        &self.seeker
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
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PubSubContext {
    channel: String,
    from_pattern: bool,
}

impl PubSubContext {
    /// Constructs a context directly from its native fields (mainly for tests).
    #[must_use]
    pub fn new(channel: impl Into<String>, from_pattern: bool) -> Self {
        Self {
            channel: channel.into(),
            from_pattern,
        }
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
        }
    }
}

/// Compile-time [`Field`] keys, one per native field, read with `ctx.context(key)`.
///
/// Each key is a zero-sized selector implementing [`Field`] only for the context types that carry
/// its field, so applying a key to the wrong transport's context is a compile error. A key that
/// also implements [`ContextField`](ruststream::ContextField) can be bound as a `Ctx<K>` handler
/// parameter; those read the per-delivery context, so a page body reaches its own fields through
/// `ctx.context(..)` instead.
pub mod keys {
    use ruststream::ContextField;

    use super::{
        Field, PubSubContext, RedisGroupPosition, RedisGroupSeeker, StreamBatchContext,
        StreamContext,
    };

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

    /// Reads the group's reposition handle off a stream context, per delivery or per page.
    ///
    /// The handle is subscription-scoped (resolved once, when the subscription opens), which is
    /// why it is the one field both context types carry. As a `Ctx<SeekHandle>` parameter it binds
    /// the per-delivery context; a page body reads it with `ctx.context(SeekHandle)` off
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

    /// Reads the consumer group off a stream context, per delivery or per page.
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
    use ruststream::{ContextField, Field};

    #[test]
    fn pubsub_keys_read_channel_and_pattern_flag() {
        let exact = PubSubContext::new("events", false);
        assert_eq!(Channel.get(&exact), "events");
        assert!(!FromPattern.get(&exact));

        let matched = PubSubContext::new("events.user", true);
        assert_eq!(Channel.get(&matched), "events.user");
        assert!(FromPattern.get(&matched));
    }

    #[test]
    fn pubsub_context_field_keys_yield_owned_values() {
        let pubsub = PubSubContext::new("orders.eu", true);
        assert_eq!(
            <Channel as ContextField>::read(Channel, &pubsub),
            "orders.eu".to_owned()
        );
        assert!(<FromPattern as ContextField>::read(FromPattern, &pubsub));
    }
}
