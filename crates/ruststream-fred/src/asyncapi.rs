//! What this crate adds to the generated `AsyncAPI` document.
//!
//! The specification's `redis` binding is empty: all four of its objects carry no fields, so
//! nothing a Redis service knows has a lawful place under the `redis` key. The crate writes an
//! extension beside it instead, `x-ruststream-redis`, which the specification allows at exactly
//! the level a binding sits at.
//!
//! The body is the descriptor's or the policy's own vocabulary: which Redis structure carries the
//! messages, the consumer group and consumer a stream subscription reads through, its read mode
//! and idle threshold, the reliability of a list, the delivery mode of a channel. A publisher also
//! names where it lands, in the word Redis uses for it: the key it writes into, the channel it
//! broadcasts on. Everything in it is computed from the descriptor, the policy and the destination
//! the mount site resolved, because the document is built before anything connects, and nothing in
//! it is a credential: the document is published and shared.

use ruststream::asyncapi::{Binding, Bindings};
use serde::Serialize;

use crate::envelope::SharedEnvelope;

/// The extension key the crate's own vocabulary travels under, at the level a `redis` binding
/// would sit at if the specification gave it any fields.
const REDIS_EXTENSION: &str = "x-ruststream-redis";

/// Which Redis structure carries the messages of a channel.
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum Form {
    /// A stream read through a consumer group (`XADD` / `XREADGROUP`).
    Stream,
    /// A list used as a work queue (`LPUSH` / `BRPOP`).
    List,
    /// Pub/Sub fan-out (`PUBLISH` / `SUBSCRIBE`).
    PubSub,
}

/// How a stream subscription reads its entries.
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum ReadMode {
    /// The fresh tail (`XREADGROUP >`).
    Fresh,
    /// Stale pending entries of other consumers (`XAUTOCLAIM`).
    Reclaim,
    /// Both in one read (`XREADGROUP ... CLAIM`, Redis 8.4 and later).
    Claiming,
}

/// A stream subscription's own settings.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct StreamSubscription<'a> {
    form: Form,
    #[serde(skip_serializing_if = "Option::is_none")]
    group: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    consumer: Option<&'a str>,
    read_mode: ReadMode,
    /// How long an entry has to have been pending before the subscription claims it, on the two
    /// modes that claim.
    #[serde(skip_serializing_if = "Option::is_none")]
    min_idle_ms: Option<u64>,
}

/// A list subscription's own settings.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ListSubscription<'a> {
    form: Form,
    /// Whether entries move to a processing list and are removed on acknowledgement
    /// (at-least-once), rather than popped outright (at-most-once).
    reliable: bool,
    /// The processing list an unacknowledged entry sits on, in reliable mode.
    #[serde(skip_serializing_if = "Option::is_none")]
    processing: Option<&'a str>,
    envelope: Envelope,
}

/// A Pub/Sub subscription's own settings.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PubSubSubscription {
    form: Form,
    /// `classic` (`SUBSCRIBE`) or `sharded` (`SSUBSCRIBE`).
    mode: &'static str,
    /// Whether the address is a glob the server matches channel names against.
    pattern: bool,
    envelope: Envelope,
}

/// Where a publish through a policy lands, named the way Redis names it.
///
/// The channel's `address` reports the same string wherever the mount site resolved one; this is
/// what a reader has when a naming transform decides the destination per delivery and the document
/// reports no address at all.
#[derive(Debug, Serialize)]
#[serde(untagged)]
pub(crate) enum Target<'a> {
    /// The stream or list key the publisher writes into.
    Key { key: &'a str },
    /// The channel the publisher broadcasts on.
    Channel { channel: &'a str },
}

/// A publisher's own settings, per form.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Publish<'a> {
    form: Form,
    #[serde(flatten)]
    target: Target<'a>,
    /// The Pub/Sub delivery mode a `PUBLISH` goes out in.
    #[serde(skip_serializing_if = "Option::is_none")]
    mode: Option<&'static str>,
    /// The expiry re-armed on the list key by every push.
    #[serde(skip_serializing_if = "Option::is_none")]
    ttl_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    envelope: Option<Envelope>,
}

/// How headers travel beside the payload on the two forms that frame them.
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Envelope {
    /// The media type of the framing: `application/octet-stream` for the default binary frame,
    /// the codec's own media type for a codec-serialized envelope.
    content_type: &'static str,
}

impl Envelope {
    /// The framing a descriptor or a policy carrying `codec` writes.
    pub(crate) fn of(codec: Option<&SharedEnvelope>) -> Self {
        Self {
            content_type: codec.map_or("application/octet-stream", |codec| codec.content_type()),
        }
    }
}

impl<'a> StreamSubscription<'a> {
    pub(crate) const fn new(
        group: Option<&'a str>,
        consumer: Option<&'a str>,
        read_mode: ReadMode,
        min_idle_ms: Option<u64>,
    ) -> Self {
        Self {
            form: Form::Stream,
            group,
            consumer,
            read_mode,
            min_idle_ms,
        }
    }
}

impl<'a> ListSubscription<'a> {
    pub(crate) const fn new(
        reliable: bool,
        processing: Option<&'a str>,
        envelope: Envelope,
    ) -> Self {
        Self {
            form: Form::List,
            reliable,
            processing,
            envelope,
        }
    }
}

impl PubSubSubscription {
    pub(crate) const fn new(mode: &'static str, pattern: bool, envelope: Envelope) -> Self {
        Self {
            form: Form::PubSub,
            mode,
            pattern,
            envelope,
        }
    }
}

impl<'a> Publish<'a> {
    /// An `XADD` publisher, which carries no settings beyond the key it appends to: a stream entry
    /// holds its headers as fields, so there is no envelope either.
    pub(crate) const fn stream(key: &'a str) -> Self {
        Self {
            form: Form::Stream,
            target: Target::Key { key },
            mode: None,
            ttl_ms: None,
            envelope: None,
        }
    }

    pub(crate) const fn list(key: &'a str, ttl_ms: Option<u64>, envelope: Envelope) -> Self {
        Self {
            form: Form::List,
            target: Target::Key { key },
            mode: None,
            ttl_ms,
            envelope: Some(envelope),
        }
    }

    pub(crate) const fn pubsub(channel: &'a str, mode: &'static str, envelope: Envelope) -> Self {
        Self {
            form: Form::PubSub,
            target: Target::Channel { channel },
            mode: Some(mode),
            ttl_ms: None,
            envelope: Some(envelope),
        }
    }
}

/// Wraps `body` as the crate's channel extension.
///
/// A body that fails to serialize is a body the document goes without: a broker never holds up a
/// service over a description of itself.
pub(crate) fn channel<T: Serialize>(body: &T) -> Bindings {
    Binding::extension(REDIS_EXTENSION, body)
        .map_or_else(|_| Bindings::new(), |binding| Bindings::new().with(binding))
}
