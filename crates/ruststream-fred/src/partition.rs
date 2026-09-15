//! The partition key: a per-message option on this crate's publishers, set by a step on the
//! publish builder.
//!
//! Redis has no partition of its own, so the resolved key travels as the well-known
//! [`PARTITION_KEY_HEADER`] header - the wire form the consumer side's
//! [`Partitioned`](ruststream::Partitioned) reads back, on all three transports.

use std::borrow::Cow;

use ruststream::HeaderMap;
use ruststream::runtime::{PublishBuilder, PublishSink};

use crate::message::PARTITION_KEY_HEADER;

/// The per-message settings of every publisher and transaction in this crate.
///
/// One field, the partition key, and it is optional like every field of an options type: a publish
/// that names no step carries no options at all, and nothing is written into its headers.
///
/// The type is the bound that keeps this crate's builder steps off another broker's publisher, and
/// it is what a handler body names when it adjusts a setting
/// (`Out<impl Publisher<Options = RedisPublishOptions>, Marker>`). A test reads the value back off
/// the slot view with `with_options`.
///
/// # Examples
///
/// ```
/// use ruststream_fred::RedisPublishOptions;
///
/// let options = RedisPublishOptions {
///     partition_key: Some(b"tenant-a".to_vec()),
/// };
/// assert_eq!(options.partition_key.as_deref(), Some(b"tenant-a".as_slice()));
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RedisPublishOptions {
    /// The key this one message is partitioned by, or `None` to send it unkeyed.
    ///
    /// Opaque bytes: the runtime hashes them to pick a dispatch lane, and never interprets them.
    pub partition_key: Option<Vec<u8>>,
}

/// The steps this crate adds to the publish builder.
///
/// Import it from any of this crate's preludes to reach [`partition_key`](Self::partition_key) on
/// a publish over a Redis publisher. The impl is bounded on [`RedisPublishOptions`], so the step
/// does not appear on a builder over another broker's publisher.
///
/// # Examples
///
/// ```no_run
/// use ruststream_fred::stream::prelude::*;
/// use serde::Serialize;
///
/// #[derive(Outgoing, Serialize)]
/// #[outgoing(name = "orders")]
/// struct Order {
///     id: u64,
/// }
///
/// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
/// let connected = RedisBroker::standalone("redis://localhost:6379").connect().await?;
/// let publisher = connected.publisher();
///
/// // Every order of one tenant lands in the same `workers(n, by_key)` lane, so their relative
/// // order is preserved while other tenants run in parallel.
/// publisher.message(&Order { id: 7 }).partition_key("tenant-a").publish().await?;
/// publisher.message(&Order { id: 8 }).partition_key("tenant-a").publish().await?;
/// # Ok(())
/// # }
/// ```
pub trait RedisPublishSteps {
    /// Sends this one message under `key`, whatever the rest of the chain says.
    ///
    /// The key feeds the runtime's keyed worker lanes (`workers(n, by_key)`): deliveries sharing a
    /// key are dispatched to the same lane, so their relative order survives concurrency. It
    /// reaches the consumer as the [`PARTITION_KEY_HEADER`](crate::PARTITION_KEY_HEADER) header,
    /// written over whatever the call site itself put under that name.
    #[must_use]
    fn partition_key(self, key: impl AsRef<[u8]>) -> Self;
}

impl<Sink, Body, Enc, Hdrs, Dest> RedisPublishSteps for PublishBuilder<Sink, Body, Enc, Hdrs, Dest>
where
    Sink: PublishSink<Options = RedisPublishOptions>,
{
    fn partition_key(mut self, key: impl AsRef<[u8]>) -> Self {
        self.options_mut()
            .get_or_insert_with(RedisPublishOptions::default)
            .partition_key = Some(key.as_ref().to_vec());
        self
    }
}

/// The headers a publish leaves with, once `options` are resolved over the ones it carries.
///
/// A step wins over the header the call site wrote itself, because it names this one message while
/// the header map may be a contract the message type declares. A publish no step touched keeps its
/// headers untouched, so writing [`PARTITION_KEY_HEADER`] by hand stays the portable spelling.
pub(crate) fn resolved_headers<'m>(
    headers: &'m HeaderMap,
    options: Option<&RedisPublishOptions>,
) -> Cow<'m, HeaderMap> {
    let Some(key) = options.and_then(|options| options.partition_key.as_deref()) else {
        return Cow::Borrowed(headers);
    };
    let mut resolved = headers.clone();
    resolved.insert(PARTITION_KEY_HEADER, key.to_vec());
    Cow::Owned(resolved)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_unstepped_publish_keeps_its_own_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(PARTITION_KEY_HEADER, "call-site");

        let resolved = resolved_headers(&headers, None);
        assert!(matches!(resolved, Cow::Borrowed(_)));
        assert_eq!(
            resolved.get(PARTITION_KEY_HEADER),
            Some(b"call-site".as_slice())
        );
    }

    #[test]
    fn a_step_writes_over_the_call_sites_own_header() {
        let mut headers = HeaderMap::new();
        headers.insert(PARTITION_KEY_HEADER, "call-site");
        headers.insert("trace-id", "abc");

        let options = RedisPublishOptions {
            partition_key: Some(b"tenant-a".to_vec()),
        };
        let resolved = resolved_headers(&headers, Some(&options));

        assert_eq!(
            resolved.get(PARTITION_KEY_HEADER),
            Some(b"tenant-a".as_slice())
        );
        // Unrelated entries travel untouched next to the key.
        assert_eq!(resolved.get("trace-id"), Some(b"abc".as_slice()));
    }
}
