//! Redis Pub/Sub transport: fire-and-forget fan-out with no acknowledgement.
//!
//! Unlike Streams, Pub/Sub has no durability, no consumer groups, and no ack: a message reaches
//! whichever subscribers are connected at publish time, and `ack` / `nack` report
//! [`AckError::Unsupported`]. Two delivery modes exist, explicit because they do not interoperate:
//!
//! * [`PubSubMode::Classic`] - `SUBSCRIBE` / `PUBLISH`, broadcast to every node; supports patterns
//!   (`PSUBSCRIBE`). The only option on standalone and sentinel.
//! * [`PubSubMode::Sharded`] - `SSUBSCRIBE` / `SPUBLISH` (Redis 7+), slot-local so it scales across
//!   a cluster, but has no pattern support.
//!
//! Headers travel in a frame around the payload (see [`crate::envelope`]): a lossless binary frame
//! by default, or a readable codec-serialized envelope when a codec is set with
//! [`RedisPubSub::codec`] / [`RedisPubSubPublish::codec`].

use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use bytes::Bytes;
use fred::clients::Client;
use fred::interfaces::{ClientLike, PubsubInterface};
use fred::types::{Message, MessageKind};
use futures::Stream;
use futures::stream::unfold;
use ruststream::codec::Codec;
use ruststream::{
    AckError, Headers, IncomingMessage, OutgoingMessage, PairError, Partitioned, PublishPolicy,
    Publisher, SubscriptionSource,
};
use tokio::sync::broadcast::{Receiver, error::RecvError};

use crate::broker::{ConnectedRedisBroker, RedisCore};
use crate::envelope::{SharedEnvelope, frame, unframe};
use crate::{error::RedisError, message::PARTITION_KEY_HEADER};

/// Pub/Sub delivery mode. Defaults to [`Classic`](Self::Classic).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum PubSubMode {
    /// `SUBSCRIBE` / `PUBLISH`: cluster-wide broadcast, pattern-capable, does not scale by slot.
    #[default]
    Classic,
    /// `SSUBSCRIBE` / `SPUBLISH` (Redis 7+): slot-local sharded delivery, no patterns.
    Sharded,
}

/// Describes one Pub/Sub subscription against a [`ConnectedRedisBroker`].
///
/// # Examples
///
/// ```
/// use ruststream_fred::{PubSubMode, RedisPubSub};
///
/// let classic = RedisPubSub::new("events");
/// let sharded = RedisPubSub::new("events").mode(PubSubMode::Sharded);
/// let pattern = RedisPubSub::new("events.*").pattern(); // classic only
/// # let _ = (classic, sharded, pattern);
/// ```
#[derive(Clone)]
#[must_use]
pub struct RedisPubSub {
    channel: String,
    mode: PubSubMode,
    pattern: bool,
    codec: Option<SharedEnvelope>,
}

impl Debug for RedisPubSub {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisPubSub")
            .field("channel", &self.channel)
            .field("mode", &self.mode)
            .field("pattern", &self.pattern)
            .field("codec", &self.codec.is_some())
            .finish()
    }
}

impl RedisPubSub {
    /// A subscription on `channel` (an exact channel by default; see [`pattern`](Self::pattern)).
    pub fn new(channel: impl Into<String>) -> Self {
        Self {
            channel: channel.into(),
            mode: PubSubMode::default(),
            pattern: false,
            codec: None,
        }
    }

    /// Sets the delivery mode. Defaults to [`PubSubMode::Classic`].
    pub const fn mode(mut self, mode: PubSubMode) -> Self {
        self.mode = mode;
        self
    }

    /// Treats the channel as a glob pattern (`PSUBSCRIBE`). Classic mode only; combining it with
    /// [`PubSubMode::Sharded`] is rejected at subscribe time.
    pub const fn pattern(mut self) -> Self {
        self.pattern = true;
        self
    }

    /// Decodes the header/payload envelope with `codec` (must match the publisher). Without it the
    /// default lossless binary framing is used.
    pub fn codec(mut self, codec: impl Codec + 'static) -> Self {
        self.codec = Some(Arc::new(codec));
        self
    }

    /// The channel (or pattern) this subscription listens on.
    #[must_use]
    pub fn channel(&self) -> &str {
        &self.channel
    }

    pub(crate) const fn delivery_mode(&self) -> PubSubMode {
        self.mode
    }

    pub(crate) const fn is_pattern(&self) -> bool {
        self.pattern
    }

    pub(crate) fn codec_handle(&self) -> Option<SharedEnvelope> {
        self.codec.clone()
    }

    pub(crate) fn validate(&self) -> Result<(), RedisError> {
        if self.pattern && matches!(self.mode, PubSubMode::Sharded) {
            return Err(RedisError::InvalidOptions(
                "pattern subscriptions are classic-only; sharded pub/sub has no PSUBSCRIBE"
                    .to_owned(),
            ));
        }
        Ok(())
    }
}

impl SubscriptionSource<ConnectedRedisBroker> for RedisPubSub {
    type Subscriber = RedisPubSubSubscriber;

    fn name(&self) -> &str {
        self.channel()
    }

    async fn subscribe(
        self,
        connected: &ConnectedRedisBroker,
    ) -> Result<Self::Subscriber, RedisError> {
        connected.subscribe_pubsub(self).await
    }
}

#[cfg(feature = "testing")]
impl SubscriptionSource<crate::testing::ConnectedRedisTestBroker> for RedisPubSub {
    type Subscriber = crate::testing::RedisTestSubscriber;

    fn name(&self) -> &str {
        self.channel()
    }

    async fn subscribe(
        self,
        connected: &crate::testing::ConnectedRedisTestBroker,
    ) -> Result<Self::Subscriber, RedisError> {
        connected.subscribe(self.channel()).await
    }
}

/// A Pub/Sub subscription backed by a dedicated `fred` client, so its message stream and channel
/// state are isolated from other subscribers and from the publishing pool.
pub struct RedisPubSubSubscriber {
    client: Client,
    rx: Receiver<Message>,
    codec: Option<SharedEnvelope>,
}

impl Debug for RedisPubSubSubscriber {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisPubSubSubscriber")
            .finish_non_exhaustive()
    }
}

impl RedisPubSubSubscriber {
    pub(crate) fn new(
        client: Client,
        rx: Receiver<Message>,
        codec: Option<SharedEnvelope>,
    ) -> Self {
        Self { client, rx, codec }
    }
}

impl Drop for RedisPubSubSubscriber {
    fn drop(&mut self) {
        // The dedicated client owns a background connection task; close it on a detached task since
        // `drop` cannot await.
        let client = self.client.clone();
        tokio::spawn(async move {
            let _ = client.quit().await;
        });
    }
}

fn to_message(msg: &Message, codec: Option<&SharedEnvelope>) -> RedisPubSubMessage {
    let raw = msg.value.as_bytes().unwrap_or(&[]);
    let (payload, headers) = unframe(codec, raw);
    RedisPubSubMessage {
        channel: msg.channel.to_string(),
        // `PMessage` is the delivery kind for a `PSUBSCRIBE` match; the message's own channel is the
        // concrete one matched, which differs from the subscription's glob pattern.
        pattern: matches!(msg.kind, MessageKind::PMessage),
        payload,
        headers,
    }
}

impl ruststream::Subscriber for RedisPubSubSubscriber {
    type Message = RedisPubSubMessage;
    type Error = RedisError;

    /// Yields one message per Pub/Sub delivery.
    ///
    /// # Cancel safety
    ///
    /// Dropping the returned stream between items is safe. Because Pub/Sub has no buffering, any
    /// message published while no stream is polling is lost (this is Redis Pub/Sub semantics, not a
    /// limitation of this client).
    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        let codec = self.codec.clone();
        unfold((&mut self.rx, codec), |(rx, codec)| async move {
            loop {
                match rx.recv().await {
                    Ok(msg) => {
                        let message = to_message(&msg, codec.as_ref());
                        return Some((Ok(message), (rx, codec)));
                    }
                    // The receiver fell behind the broadcast buffer; skip the gap and keep reading.
                    Err(RecvError::Lagged(_)) => {}
                    Err(RecvError::Closed) => return None,
                }
            }
        })
    }
}

/// A Pub/Sub delivery. `ack` / `nack` are unsupported (Pub/Sub has no acknowledgement).
pub struct RedisPubSubMessage {
    channel: String,
    /// Whether this delivery arrived through a `PSUBSCRIBE` pattern match (vs an exact subscribe).
    pattern: bool,
    payload: Bytes,
    headers: Headers,
}

impl Debug for RedisPubSubMessage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisPubSubMessage")
            .field("channel", &self.channel)
            .field("pattern", &self.pattern)
            .field("payload_len", &self.payload.len())
            .finish_non_exhaustive()
    }
}

impl RedisPubSubMessage {
    /// The channel this message arrived on.
    ///
    /// For a pattern ([`RedisPubSub::pattern`]) subscription this is the concrete channel the
    /// message was published to, which differs from the glob the subscription registered.
    #[must_use]
    pub fn channel(&self) -> &str {
        &self.channel
    }

    /// Whether this delivery arrived through a `PSUBSCRIBE` pattern match rather than an exact
    /// channel subscribe.
    #[must_use]
    pub fn from_pattern(&self) -> bool {
        self.pattern
    }
}

impl IncomingMessage for RedisPubSubMessage {
    fn payload(&self) -> &[u8] {
        &self.payload
    }

    fn headers(&self) -> &Headers {
        &self.headers
    }

    async fn ack(self) -> Result<(), AckError> {
        Err(AckError::Unsupported)
    }

    async fn nack(self, _requeue: bool) -> Result<(), AckError> {
        Err(AckError::Unsupported)
    }
}

impl Partitioned for RedisPubSubMessage {
    fn partition_key(&self) -> Option<&[u8]> {
        self.headers().get(PARTITION_KEY_HEADER)
    }
}

/// The declaration half of the Pub/Sub publisher: delivery mode and envelope codec, no connection.
///
/// Constructible anywhere (in a router definition, in configuration), it pairs into a
/// [`RedisPubSubPublisher`] against a [`ConnectedRedisBroker`]. The publish mode must match how
/// subscribers subscribed: a sharded publish only reaches sharded subscribers.
///
/// # Examples
///
/// ```
/// use ruststream_fred::{PubSubMode, RedisPubSubPublish};
///
/// let classic = RedisPubSubPublish::default();
/// let sharded = RedisPubSubPublish::new().mode(PubSubMode::Sharded);
/// # let _ = (classic, sharded);
/// ```
#[derive(Clone, Default)]
#[must_use]
pub struct RedisPubSubPublish {
    mode: PubSubMode,
    codec: Option<SharedEnvelope>,
}

impl Debug for RedisPubSubPublish {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisPubSubPublish")
            .field("mode", &self.mode)
            .field("codec", &self.codec.is_some())
            .finish()
    }
}

impl RedisPubSubPublish {
    /// A classic-mode policy with the default binary framing. Equivalent to [`Self::default`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the publish mode. Defaults to [`PubSubMode::Classic`].
    pub const fn mode(mut self, mode: PubSubMode) -> Self {
        self.mode = mode;
        self
    }

    /// Serializes the header/payload envelope with `codec` (must match the subscriber). Without it
    /// the default lossless binary framing is used.
    pub fn codec(mut self, codec: impl Codec + 'static) -> Self {
        self.codec = Some(Arc::new(codec));
        self
    }
}

impl PublishPolicy<ConnectedRedisBroker> for RedisPubSubPublish {
    type Live = RedisPubSubPublisher;

    async fn pair(self, connected: &ConnectedRedisBroker) -> Result<Self::Live, PairError> {
        Ok(connected.pubsub_publisher(self))
    }
}

/// Publishes Pub/Sub messages with `PUBLISH` (classic) or `SPUBLISH` (sharded): a
/// [`RedisPubSubPublish`] policy paired with a connection.
///
/// Obtain it from
/// [`ConnectedRedisBroker::pubsub_publisher`](crate::ConnectedRedisBroker::pubsub_publisher), or
/// by pairing the policy. Like every publisher here it may outlive the connection, so publishing
/// after shutdown reports [`RedisError::ShutDown`].
#[derive(Clone)]
pub struct RedisPubSubPublisher {
    core: Arc<RedisCore>,
    mode: PubSubMode,
    codec: Option<SharedEnvelope>,
}

impl Debug for RedisPubSubPublisher {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisPubSubPublisher")
            .field("mode", &self.mode)
            .field("codec", &self.codec.is_some())
            .finish_non_exhaustive()
    }
}

impl RedisPubSubPublisher {
    pub(crate) fn new(core: Arc<RedisCore>, publish: RedisPubSubPublish) -> Self {
        Self {
            core,
            mode: publish.mode,
            codec: publish.codec,
        }
    }
}

impl Publisher for RedisPubSubPublisher {
    type Error = RedisError;

    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        let pool = self.core.pool()?;
        let client = pool.next();
        let channel = msg.name().to_owned();
        let body = frame(self.codec.as_ref(), msg.payload(), msg.headers());
        let _: i64 = match self.mode {
            PubSubMode::Classic => client.publish(channel, body).await,
            PubSubMode::Sharded => client.spublish(channel, body).await,
        }
        .map_err(RedisError::publish)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::PubSubContext;
    use ruststream::BuildContext;

    #[test]
    fn build_context_reads_channel_and_pattern_flag() {
        let exact = RedisPubSubMessage {
            channel: "events".to_owned(),
            pattern: false,
            payload: Bytes::from_static(b"{}"),
            headers: Headers::new(),
        };
        let cx = PubSubContext::build(&exact);
        assert_eq!(cx.channel(), "events");
        assert!(!cx.from_pattern());

        let matched = RedisPubSubMessage {
            channel: "events.user".to_owned(),
            pattern: true,
            payload: Bytes::from_static(b"{}"),
            headers: Headers::new(),
        };
        assert!(PubSubContext::build(&matched).from_pattern());
    }

    #[test]
    fn pattern_with_sharded_is_rejected() {
        let err = RedisPubSub::new("e.*")
            .mode(PubSubMode::Sharded)
            .pattern()
            .validate()
            .unwrap_err();
        assert!(matches!(err, RedisError::InvalidOptions(msg) if msg.contains("classic-only")));
    }

    #[test]
    fn classic_pattern_validates() {
        RedisPubSub::new("e.*").pattern().validate().expect("ok");
    }
}
