//! What a `.pipeline()` descriptor opens: the form's own subscription, with a window.

use std::fmt::{Debug, Formatter};
use std::future::{Future, ready};
use std::sync::Arc;

use futures::Stream;
#[cfg(feature = "asyncapi")]
use ruststream::asyncapi::Bindings;
use ruststream::{
    AddressedCopies, RedeliveryAddress, RedeliveryAddressed, RetryDeclaration, Seekable,
    Subscriber, SubscriptionSource,
};

use super::forms::{ListForm, PubSubForm, StreamForm};
use super::window::{Form, Window};
use super::{Pipelined, RoundMessage, WindowMode};
use crate::broker::ConnectedRedisBroker;
use crate::error::RedisError;
use crate::list::{ListReader, RedisList};
use crate::pubsub::{PubSubWire, RedisPubSub};
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
        // the window still owes runs on the runtime, and where there is none it cannot run at all.
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let window = Arc::clone(&self.window);
            runtime.spawn(async move { window.drain().await });
        }
    }
}

impl Subscriber for PipelinedSubscriber<RedisSubscriber, StreamForm> {
    type Message = RoundMessage<StreamForm>;
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

/// Repositions the consumer group as the subscription without a window does, which is what a
/// mount site's `start_at(..)` seeks through.
impl Seekable for PipelinedSubscriber<RedisSubscriber, StreamForm> {
    type Seeker = RedisGroupSeeker;

    fn seeker(&self) -> RedisGroupSeeker {
        self.inner.seeker()
    }
}

/// A Pub/Sub subscription's dedicated reader, as a pipelined subscription holds it.
pub struct ChannelReader(PubSubWire);

impl Debug for ChannelReader {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChannelReader").finish_non_exhaustive()
    }
}

impl Subscriber for PipelinedSubscriber<ChannelReader, PubSubForm> {
    type Message = RoundMessage<PubSubForm>;
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

impl Subscriber for PipelinedSubscriber<ListReader, ListForm> {
    type Message = RoundMessage<ListForm>;
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
    ($broker:ty, $descriptor:ty) => {
        impl<Mode: WindowMode> RedeliveryAddressed<$broker> for Pipelined<$descriptor, Mode> {
            fn redelivery_address(
                &self,
                connected: &$broker,
            ) -> impl Future<Output = Result<RedeliveryAddress, RedisError>> {
                self.descriptor().redelivery_address(connected)
            }
        }
    };
}

impl<Mode: WindowMode> SubscriptionSource<ConnectedRedisBroker> for Pipelined<RedisStream, Mode> {
    type Subscriber = PipelinedSubscriber<RedisSubscriber, StreamForm>;
    type Copies = AddressedCopies;

    delegates!(ConnectedRedisBroker, key);

    async fn subscribe(
        self,
        connected: &ConnectedRedisBroker,
    ) -> Result<Self::Subscriber, RedisError> {
        let inner = connected.subscribe(self.into_descriptor()).await?;
        let window = Window::new(
            inner.round_form(),
            inner.round_client(),
            Mode::ATOMIC,
            inner.key(),
            prefetch(),
        );
        Ok(PipelinedSubscriber::new(inner, window))
    }
}

impl<Mode: WindowMode> SubscriptionSource<ConnectedRedisBroker> for Pipelined<RedisPubSub, Mode> {
    type Subscriber = PipelinedSubscriber<ChannelReader, PubSubForm>;
    type Copies = AddressedCopies;

    delegates!(ConnectedRedisBroker, channel);

    async fn subscribe(
        self,
        connected: &ConnectedRedisBroker,
    ) -> Result<Self::Subscriber, RedisError> {
        let channel = self.descriptor().channel().to_owned();
        let inner = connected.open_pubsub(self.into_descriptor()).await?;
        // Pub/Sub reads one message at a time, so the window leaves when nothing is in flight;
        // the capacity only caps a window that never drains.
        let window = Window::new(
            PubSubForm,
            inner.round_client(),
            Mode::ATOMIC,
            channel,
            prefetch(),
        );
        Ok(PipelinedSubscriber::new(ChannelReader(inner), window))
    }
}

impl<Mode: WindowMode> SubscriptionSource<ConnectedRedisBroker> for Pipelined<RedisList, Mode> {
    type Subscriber = PipelinedSubscriber<ListReader, ListForm>;
    type Copies = AddressedCopies;

    delegates!(ConnectedRedisBroker, key);

    // Opening a list issues no command, so there is nothing to suspend on.
    fn subscribe(
        self,
        connected: &ConnectedRedisBroker,
    ) -> impl Future<Output = Result<Self::Subscriber, RedisError>> {
        ready(connected.open_list(self.into_descriptor()).map(|wire| {
            let window = Window::new(
                wire.round_form(),
                wire.round_client(),
                Mode::ATOMIC,
                wire.key(),
                prefetch(),
            );
            PipelinedSubscriber::new(ListReader::new(wire), window)
        }))
    }
}

addressed!(ConnectedRedisBroker, RedisStream);
addressed!(ConnectedRedisBroker, RedisList);
addressed!(ConnectedRedisBroker, RedisPubSub);

#[cfg(feature = "testing")]
mod testing {
    use std::future::Future;

    use futures::Stream;
    #[cfg(feature = "asyncapi")]
    use ruststream::asyncapi::Bindings;
    use ruststream::{
        AddressedCopies, RedeliveryAddress, RedeliveryAddressed, RetryDeclaration, Subscriber,
        SubscriptionSource,
    };

    use super::{PipelinedSubscriber, prefetch};
    use crate::error::RedisError;
    use crate::list::RedisList;
    use crate::pipeline::{Pipelined, RoundMessage, TestForm, Window, WindowMode};
    use crate::pubsub::RedisPubSub;
    use crate::stream::RedisStream;
    use crate::testing::{ConnectedRedisTestBroker, RedisTestSubscriber};

    impl Subscriber for PipelinedSubscriber<RedisTestSubscriber, TestForm> {
        type Message = RoundMessage<TestForm>;
        type Error = RedisError;

        fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
            self.inner.round_stream(&self.window)
        }
    }

    /// The stand-in mounts a pipelined descriptor the way it mounts the descriptor, with a window
    /// over its own pool.
    macro_rules! stand_in {
        ($descriptor:ty, $name:ident) => {
            impl<Mode: WindowMode> SubscriptionSource<ConnectedRedisTestBroker>
                for Pipelined<$descriptor, Mode>
            {
                type Subscriber = PipelinedSubscriber<RedisTestSubscriber, TestForm>;
                type Copies = AddressedCopies;

                delegates!(ConnectedRedisTestBroker, $name);

                async fn subscribe(
                    self,
                    connected: &ConnectedRedisTestBroker,
                ) -> Result<Self::Subscriber, RedisError> {
                    let name = self.descriptor().$name().to_owned();
                    let inner = SubscriptionSource::<ConnectedRedisTestBroker>::subscribe(
                        self.into_descriptor(),
                        connected,
                    )
                    .await?;
                    let window = Window::new(
                        TestForm,
                        connected.pool_handle()?.next().clone(),
                        Mode::ATOMIC,
                        name,
                        prefetch(),
                    );
                    Ok(PipelinedSubscriber::new(inner, window))
                }
            }

            addressed!(ConnectedRedisTestBroker, $descriptor);
        };
    }

    stand_in!(RedisStream, key);
    stand_in!(RedisList, key);
    stand_in!(RedisPubSub, channel);
}
