//! What a `.pipeline()` subscription opens: the form's own subscription, with a window.

use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::Arc;

use futures::Stream;
#[cfg(feature = "asyncapi")]
use ruststream::asyncapi::Bindings;
use ruststream::{
    AddressedCopies, BatchSubscriber, NamedCopies, RedeliveryAddress, RedeliveryAddressed,
    RetryDeclaration, Seekable, Subscriber, SubscriptionSource,
};

use super::forms::{ListForm, PubSubForm, StreamForm};
use super::window::{Form, Window};
use super::{Pipelined, RoundMessage, WindowMode};
use crate::broker::ConnectedRedisBroker;
use crate::envelope::{Envelope, EnvelopeChoice};
use crate::error::RedisError;
use crate::list::{ListReader, RedisList};
use crate::pubsub::{PubSubWire, RedisPubSub, RedisPubSubPattern};
use crate::seek::RedisGroupSeeker;
use crate::stream::RedisStream;
use crate::subscriber::{PREFETCH, RedisSubscriber};

/// The window's capacity on the single-delivery path: the read's `COUNT`.
fn prefetch() -> usize {
    usize::try_from(PREFETCH).unwrap_or(usize::MAX)
}

/// A pipelined subscription: the form's own subscriber and the window its deliveries settle in.
///
/// The window flushes what it still owes when the subscription stops.
pub struct PipelinedSubscriber<Inner, F: Form> {
    inner: Inner,
    window: Arc<Window<F>>,
}

impl<Inner, F: Form> Debug for PipelinedSubscriber<Inner, F> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PipelinedSubscriber")
            .field("window", &self.window)
            .finish_non_exhaustive()
    }
}

impl<Inner, F: Form> PipelinedSubscriber<Inner, F> {
    fn new(inner: Inner, window: Window<F>) -> Self {
        Self {
            inner,
            window: Arc::new(window),
        }
    }
}

impl<Inner, F: Form> Drop for PipelinedSubscriber<Inner, F> {
    fn drop(&mut self) {
        // A subscription stops by being dropped, and a destructor cannot wait: the flush of what
        // the window still owes runs on the broker's runtime, wherever the drop happens.
        let window = Arc::clone(&self.window);
        self.window.spawn(async move { window.drain().await });
    }
}

impl<Env: Envelope> Subscriber for PipelinedSubscriber<RedisSubscriber<Env>, StreamForm<Env>> {
    type Message = RoundMessage<StreamForm<Env>>;
    type Error = RedisError;

    /// Yields one delivery per entry, each with a slot in the window.
    ///
    /// # Cancel safety
    ///
    /// As [`RedisSubscriber`]'s: entries fetched and not yet settled stay pending.
    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        self.inner.round_stream(&self.window)
    }
}

impl<Env: Envelope> BatchSubscriber for PipelinedSubscriber<RedisSubscriber<Env>, StreamForm<Env>> {
    type Batch = Vec<RoundMessage<StreamForm<Env>>>;

    /// Yields one batch per read, `size` being the read's `COUNT`; the batch is one segment of the
    /// window, so its settles leave as one `XACK`.
    ///
    /// # Cancel safety
    ///
    /// As [`RedisSubscriber`]'s batches: entries fetched and not yet settled stay pending.
    fn batches(
        &mut self,
        size: NonZeroUsize,
    ) -> impl Stream<Item = Result<Self::Batch, Self::Error>> + Send + '_ {
        self.inner.round_batches(&self.window, size)
    }
}

/// Repositions the consumer group as the subscription without a window does, which is what a
/// mount site's `start_at(..)` seeks through.
impl<Env: Envelope> Seekable for PipelinedSubscriber<RedisSubscriber<Env>, StreamForm<Env>> {
    type Seeker = RedisGroupSeeker;

    fn seeker(&self) -> RedisGroupSeeker {
        self.inner.seeker()
    }
}

/// A Pub/Sub subscription's dedicated reader, as a pipelined subscription holds it.
pub struct ChannelReader<Env>(PubSubWire<Env>);

impl<Env> Debug for ChannelReader<Env> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChannelReader").finish_non_exhaustive()
    }
}

impl<Env: Envelope> Subscriber for PipelinedSubscriber<ChannelReader<Env>, PubSubForm<Env>> {
    type Message = RoundMessage<PubSubForm<Env>>;
    type Error = RedisError;

    /// Yields one delivery per message, each with a slot in the window.
    ///
    /// # Cancel safety
    ///
    /// As a Pub/Sub subscription's: a message nobody is polling for is lost.
    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        self.inner.0.round_stream(&self.window)
    }
}

impl<Env: Envelope> Subscriber for PipelinedSubscriber<ListReader<Env>, ListForm<Env>> {
    type Message = RoundMessage<ListForm<Env>>;
    type Error = RedisError;

    /// Yields one delivery per entry, each with a slot in the window, claiming the read's `COUNT`
    /// at a time.
    ///
    /// # Cancel safety
    ///
    /// As [`RedisListSubscriber`](crate::RedisListSubscriber)'s: on a reliable list an entry
    /// claimed and not settled stays on the processing list.
    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        self.inner.round_stream(&self.window, prefetch())
    }
}

impl<Env: Envelope> BatchSubscriber for PipelinedSubscriber<ChannelReader<Env>, PubSubForm<Env>> {
    type Batch = Vec<RoundMessage<PubSubForm<Env>>>;

    /// Yields a batch of the messages already arrived, up to `size`; the batch is one segment of
    /// the window.
    ///
    /// # Cancel safety
    ///
    /// As a Pub/Sub subscription's: a message nobody is polling for is lost.
    fn batches(
        &mut self,
        size: NonZeroUsize,
    ) -> impl Stream<Item = Result<Self::Batch, Self::Error>> + Send + '_ {
        self.inner.0.round_batches(&self.window, size)
    }
}

impl<Env: Envelope> BatchSubscriber for PipelinedSubscriber<ListReader<Env>, ListForm<Env>> {
    type Batch = Vec<RoundMessage<ListForm<Env>>>;

    /// Yields one batch per read of up to `size` entries; the batch is one segment of the window.
    ///
    /// # Cancel safety
    ///
    /// As a list subscription's batches: on a reliable list an entry claimed and not settled
    /// stays on the processing list.
    fn batches(
        &mut self,
        size: NonZeroUsize,
    ) -> impl Stream<Item = Result<Self::Batch, Self::Error>> + Send + '_ {
        self.inner.round_batches(&self.window, size)
    }
}

/// The parts of `SubscriptionSource` every pipelined descriptor shares with the descriptor it
/// wraps: its name, its retry declaration and its document.
macro_rules! delegates {
    ($broker:ty, $name:ident) => {
        fn name(&self) -> &str {
            self.descriptor().$name()
        }

        fn declare_retry(self, declaration: &RetryDeclaration) -> Self {
            Self::wrap(SubscriptionSource::<$broker>::declare_retry(
                self.into_descriptor(),
                declaration,
            ))
        }

        #[cfg(feature = "asyncapi")]
        fn channel_bindings(&self) -> Bindings {
            SubscriptionSource::<$broker>::channel_bindings(self.descriptor())
        }
    };
}

/// Where a pipelined descriptor's copies go: where the descriptor's go.
macro_rules! addressed {
    ($descriptor:ident) => {
        impl<BrokerEnv, Env, Mode> RedeliveryAddressed<ConnectedRedisBroker<BrokerEnv>>
            for Pipelined<$descriptor<Env>, Mode>
        where
            BrokerEnv: Envelope,
            Env: EnvelopeChoice<BrokerEnv>,
            Mode: WindowMode,
        {
            fn redelivery_address(
                &self,
                connected: &ConnectedRedisBroker<BrokerEnv>,
            ) -> impl Future<Output = Result<RedeliveryAddress, RedisError>> {
                self.descriptor().redelivery_address(connected)
            }
        }
    };
}

impl<BrokerEnv, Env, Mode> SubscriptionSource<ConnectedRedisBroker<BrokerEnv>>
    for Pipelined<RedisStream<Env>, Mode>
where
    BrokerEnv: Envelope,
    Env: EnvelopeChoice<BrokerEnv>,
    Mode: WindowMode,
{
    type Subscriber =
        PipelinedSubscriber<RedisSubscriber<Env::Envelope>, StreamForm<Env::Envelope>>;
    type Copies = AddressedCopies;

    delegates!(ConnectedRedisBroker<BrokerEnv>, key);

    async fn subscribe(
        self,
        connected: &ConnectedRedisBroker<BrokerEnv>,
    ) -> Result<Self::Subscriber, RedisError> {
        let inner = connected.subscribe(self.into_descriptor()).await?;
        let window = Window::new(
            inner.round_form(),
            inner.round_client(),
            Mode::ATOMIC,
            inner.key(),
            prefetch(),
            connected.window_owner()?,
        );
        Ok(PipelinedSubscriber::new(inner, window))
    }
}

impl<BrokerEnv, Env, Mode> SubscriptionSource<ConnectedRedisBroker<BrokerEnv>>
    for Pipelined<RedisPubSub<Env>, Mode>
where
    BrokerEnv: Envelope,
    Env: EnvelopeChoice<BrokerEnv>,
    Mode: WindowMode,
{
    type Subscriber = PipelinedSubscriber<ChannelReader<Env::Envelope>, PubSubForm<Env::Envelope>>;
    type Copies = AddressedCopies;

    delegates!(ConnectedRedisBroker<BrokerEnv>, channel);

    async fn subscribe(
        self,
        connected: &ConnectedRedisBroker<BrokerEnv>,
    ) -> Result<Self::Subscriber, RedisError> {
        let channel = self.descriptor().channel().to_owned();
        let inner = connected.open_pubsub(self.into_descriptor()).await?;
        // Pub/Sub reads one message at a time, so the window leaves when nothing is in flight;
        // the capacity only caps a window that never drains.
        let window = Window::new(
            PubSubForm::default(),
            inner.round_client(),
            Mode::ATOMIC,
            channel,
            prefetch(),
            connected.window_owner()?,
        );
        Ok(PipelinedSubscriber::new(ChannelReader(inner), window))
    }
}

/// A pattern reads every channel its glob matches and names none a publish could reach, so the
/// mount site names where a retry copy goes, as on the pattern subscription without a window.
impl<BrokerEnv, Env, Mode> SubscriptionSource<ConnectedRedisBroker<BrokerEnv>>
    for Pipelined<RedisPubSubPattern<Env>, Mode>
where
    BrokerEnv: Envelope,
    Env: EnvelopeChoice<BrokerEnv>,
    Mode: WindowMode,
{
    type Subscriber = PipelinedSubscriber<ChannelReader<Env::Envelope>, PubSubForm<Env::Envelope>>;
    type Copies = NamedCopies;

    fn name(&self) -> &str {
        self.descriptor().pattern()
    }

    async fn subscribe(
        self,
        connected: &ConnectedRedisBroker<BrokerEnv>,
    ) -> Result<Self::Subscriber, RedisError> {
        let pattern = self.descriptor().pattern().to_owned();
        let inner = connected
            .open_pubsub_pattern(self.into_descriptor())
            .await?;
        // Pub/Sub reads one message at a time, as on a channel: the window leaves when nothing
        // is in flight.
        let window = Window::new(
            PubSubForm::default(),
            inner.round_client(),
            Mode::ATOMIC,
            pattern,
            prefetch(),
            connected.window_owner()?,
        );
        Ok(PipelinedSubscriber::new(ChannelReader(inner), window))
    }

    #[cfg(feature = "asyncapi")]
    fn channel_bindings(&self) -> Bindings {
        SubscriptionSource::<ConnectedRedisBroker<BrokerEnv>>::channel_bindings(self.descriptor())
    }
}

impl<BrokerEnv, Env, Mode> SubscriptionSource<ConnectedRedisBroker<BrokerEnv>>
    for Pipelined<RedisList<Env>, Mode>
where
    BrokerEnv: Envelope,
    Env: EnvelopeChoice<BrokerEnv>,
    Mode: WindowMode,
{
    type Subscriber = PipelinedSubscriber<ListReader<Env::Envelope>, ListForm<Env::Envelope>>;
    type Copies = AddressedCopies;

    delegates!(ConnectedRedisBroker<BrokerEnv>, key);

    async fn subscribe(
        self,
        connected: &ConnectedRedisBroker<BrokerEnv>,
    ) -> Result<Self::Subscriber, RedisError> {
        let wire = connected.open_list(self.into_descriptor()).await?;
        let window = Window::new(
            wire.round_form(),
            wire.round_client(),
            Mode::ATOMIC,
            wire.key(),
            prefetch(),
            connected.window_owner()?,
        );
        Ok(PipelinedSubscriber::new(ListReader::new(wire), window))
    }
}

addressed!(RedisStream);
addressed!(RedisList);
addressed!(RedisPubSub);
