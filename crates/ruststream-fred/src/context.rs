//! Optional typed per-delivery context exposing native Redis metadata, one struct per transport.
//!
//! A handler can read native Redis metadata for the message it is processing by compile-time
//! [`Field`] key, with no hashing, boxing, or downcasting. The runtime builds the context value once
//! per delivery (via [`BuildContext`]) from the concrete broker message, then the handler reads a
//! field with `ctx.context(key)`.
//!
//! This is purely additive. A handler that declares the default `()` context (the vast majority) is
//! unaffected: the blanket `impl BuildContext<M> for ()` still applies, so opting in costs nothing
//! to those who do not.
//!
//! # What is exposed
//!
//! Only genuinely-native metadata that is not already reachable off the payload or
//! [`Headers`](ruststream::Headers) is surfaced here:
//!
//! * [`StreamContext`] (Redis Streams) - the stream entry id and the consumer group. The native
//!   reclaim delivery-count and idle time stay header-surfaced
//!   ([`DELIVERY_COUNT_HEADER`](crate::DELIVERY_COUNT_HEADER) /
//!   [`IDLE_MS_HEADER`](crate::IDLE_MS_HEADER)) and are deliberately not duplicated.
//! * [`PubSubContext`] (Redis Pub/Sub) - the concrete channel the message arrived on and whether it
//!   matched through a `PSUBSCRIBE` pattern (for a pattern subscription the channel differs from the
//!   registered glob).
//!
//! # Examples
//!
//! ```
//! use ruststream::runtime::{Context, HandlerResult};
//! use ruststream_fred::context::{StreamContext, keys};
//!
//! // A handler over the Streams transport reading the native entry id and consumer group.
//! async fn handle(order: &Vec<u8>, ctx: &mut Context<'_, StreamContext>) -> HandlerResult {
//!     if let Some(id) = ctx.context(keys::EntryId) {
//!         let _ = id; // e.g. log the stream entry id `1700000000000-0`
//!     }
//!     let _group = ctx.context(keys::ConsumerGroup);
//!     HandlerResult::Ack
//! }
//! # let _ = handle;
//! ```

use ruststream::{BuildContext, Field};

use crate::message::RedisMessage;
use crate::pubsub::RedisPubSubMessage;

/// Per-delivery context for a Redis Streams delivery ([`RedisMessage`]).
///
/// Built once per delivery from the message. Read its fields by [`keys`] key off a
/// [`Context`](ruststream::runtime::Context).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StreamContext {
    entry_id: Option<String>,
    consumer_group: Option<String>,
}

impl StreamContext {
    /// Constructs a context directly from its native fields (mainly for tests).
    #[must_use]
    pub fn new(entry_id: Option<String>, consumer_group: Option<String>) -> Self {
        Self {
            entry_id,
            consumer_group,
        }
    }

    /// The stream entry id (for example `1700000000000-0`) this delivery was read at.
    #[must_use]
    pub fn entry_id(&self) -> Option<&str> {
        self.entry_id.as_deref()
    }

    /// The consumer group this delivery was read through.
    #[must_use]
    pub fn consumer_group(&self) -> Option<&str> {
        self.consumer_group.as_deref()
    }
}

impl BuildContext<RedisMessage> for StreamContext {
    fn build(msg: &RedisMessage) -> Self {
        Self {
            entry_id: msg.id().map(str::to_owned),
            consumer_group: msg.group().map(str::to_owned),
        }
    }
}

/// Per-delivery context for a Redis Pub/Sub delivery ([`RedisPubSubMessage`]).
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
    pub fn from_pattern(&self) -> bool {
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
/// Each key is a zero-sized selector implementing [`Field`] only for the context type that carries
/// its field, so applying a key to the wrong transport's context is a compile error.
pub mod keys {
    use super::{Field, PubSubContext, StreamContext};

    /// Reads the stream entry id off a [`StreamContext`].
    #[derive(Debug, Clone, Copy, Default)]
    pub struct EntryId;

    impl Field<StreamContext> for EntryId {
        type Value<'a> = Option<&'a str>;
        fn get(self, src: &StreamContext) -> Option<&str> {
            src.entry_id()
        }
    }

    /// Reads the consumer group off a [`StreamContext`].
    #[derive(Debug, Clone, Copy, Default)]
    pub struct ConsumerGroup;

    impl Field<StreamContext> for ConsumerGroup {
        type Value<'a> = Option<&'a str>;
        fn get(self, src: &StreamContext) -> Option<&str> {
            src.consumer_group()
        }
    }

    /// Reads the concrete channel off a [`PubSubContext`].
    #[derive(Debug, Clone, Copy, Default)]
    pub struct Channel;

    impl Field<PubSubContext> for Channel {
        type Value<'a> = &'a str;
        fn get(self, src: &PubSubContext) -> &str {
            src.channel()
        }
    }

    /// Reads whether a [`PubSubContext`] delivery matched through a `PSUBSCRIBE` pattern.
    #[derive(Debug, Clone, Copy, Default)]
    pub struct FromPattern;

    impl Field<PubSubContext> for FromPattern {
        type Value<'a> = bool;
        fn get(self, src: &PubSubContext) -> bool {
            src.from_pattern()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::keys::{Channel, ConsumerGroup, EntryId, FromPattern};
    use super::{PubSubContext, StreamContext};
    use ruststream::Field;

    #[test]
    fn stream_keys_read_native_fields() {
        let cx = StreamContext::new(
            Some("1700000000000-0".to_owned()),
            Some("workers".to_owned()),
        );
        assert_eq!(EntryId.get(&cx), Some("1700000000000-0"));
        assert_eq!(ConsumerGroup.get(&cx), Some("workers"));
    }

    #[test]
    fn stream_keys_absent_when_settled() {
        let cx = StreamContext::new(None, None);
        assert_eq!(EntryId.get(&cx), None);
        assert_eq!(ConsumerGroup.get(&cx), None);
    }

    #[test]
    fn pubsub_keys_read_channel_and_pattern_flag() {
        let exact = PubSubContext::new("events", false);
        assert_eq!(Channel.get(&exact), "events");
        assert!(!FromPattern.get(&exact));

        let matched = PubSubContext::new("events.user", true);
        assert_eq!(Channel.get(&matched), "events.user");
        assert!(FromPattern.get(&matched));
    }
}
