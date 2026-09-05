//! [`RedisTestSubscriber`] and [`RedisTestMessage`].
//!
//! The subscriber wraps a [`router::DeliveryReceiver`] and yields one [`RedisTestMessage`] per
//! delivery. Dropping the subscriber unregisters its subscription from the underlying
//! [`router::KeyRouter`], so handlers stop receiving messages as soon as their task finishes.

use std::future::{Future, ready};
use std::num::NonZeroUsize;
use std::sync::{Arc, OnceLock};
use std::task::Poll;

use futures::Stream;
use ruststream::{
    AckError, BatchSubscriber, HeaderMap, IncomingMessage, Partitioned, Subscriber,
    testing::Coordinator,
};

use crate::{
    error::RedisError,
    testing::{
        broker::TestBrokerState,
        router::{Delivery, DeliveryReceiver, DeliverySender, SubscriptionId},
    },
};

/// Subscriber returned by [`crate::testing::ConnectedRedisTestBroker::subscribe`].
pub struct RedisTestSubscriber {
    state: Arc<TestBrokerState>,
    id: SubscriptionId,
    rx: DeliveryReceiver,
    requeue: DeliverySender,
    /// A clone of the broker's harness coordinator, threaded into each yielded message so a requeue
    /// re-counts and a consumed delivery decrements. `None` outside a harness run.
    coordinator: Option<Coordinator>,
}

impl std::fmt::Debug for RedisTestSubscriber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisTestSubscriber")
            .finish_non_exhaustive()
    }
}

impl RedisTestSubscriber {
    pub(crate) fn new(
        state: Arc<TestBrokerState>,
        id: SubscriptionId,
        rx: DeliveryReceiver,
        requeue: DeliverySender,
    ) -> Self {
        // The harness installs its coordinator before any subscription opens, so reading it here
        // captures the live coordinator for the whole subscription.
        let coordinator = state.coordinator();
        Self {
            state,
            id,
            rx,
            requeue,
            coordinator,
        }
    }
}

impl Drop for RedisTestSubscriber {
    fn drop(&mut self) {
        self.state.router.unsubscribe(self.id);
    }
}

impl Subscriber for RedisTestSubscriber {
    type Message = RedisTestMessage;
    type Error = RedisError;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        let requeue = self.requeue.clone();
        let coordinator = self.coordinator.clone();
        // Poll the receiver in place rather than wrapping it in an owning stream, so `stream` can
        // be called again after the returned stream is dropped (the runtime and the conformance
        // helpers re-enter it per call).
        futures::stream::poll_fn(move |cx| {
            self.rx.poll_recv(cx).map(|next| {
                next.map(|delivery| {
                    Ok(RedisTestMessage::from_delivery(
                        delivery,
                        requeue.clone(),
                        coordinator.clone(),
                    ))
                })
            })
        })
    }
}

/// Message handed to handlers from a [`RedisTestSubscriber`].
///
/// `ack` consumes the handle silently; `nack(requeue = true)` re-queues the delivery on the owning
/// subscription's channel so the next handler invocation sees it again (matching the republish
/// model the real broker uses); `nack(requeue = false)` drops it.
pub struct RedisTestMessage {
    delivery: Option<Delivery>,
    requeue: DeliverySender,
    /// A clone of the broker's harness coordinator. When set, this delivery is counted in flight and
    /// decremented exactly once when the message is consumed or dropped (see the `Drop` impl). `None`
    /// outside a harness run.
    coordinator: Option<Coordinator>,
}

impl Drop for RedisTestMessage {
    /// Counts this delivery consumed exactly once: on ack, nack, or an unsettled drop (a fail-fast
    /// panic). A requeue (`nack(true)`) re-enqueues a fresh delivery first, so the in-flight count
    /// stays balanced across redelivery. `Drop` runs once per value, so the decrement is idempotent.
    fn drop(&mut self) {
        if let Some(coordinator) = &self.coordinator {
            coordinator.consumed();
        }
    }
}

impl std::fmt::Debug for RedisTestMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisTestMessage")
            .field(
                "subject",
                &self.delivery.as_ref().map(|d| d.subject.as_str()),
            )
            .finish_non_exhaustive()
    }
}

impl RedisTestMessage {
    pub(crate) fn from_delivery(
        delivery: Delivery,
        requeue: DeliverySender,
        coordinator: Option<Coordinator>,
    ) -> Self {
        Self {
            delivery: Some(delivery),
            requeue,
            coordinator,
        }
    }

    /// Returns the stream key this message was published to.
    #[must_use]
    pub fn subject(&self) -> &str {
        self.delivery
            .as_ref()
            .map(|d| d.subject.as_str())
            .unwrap_or_default()
    }
}

impl Partitioned for RedisTestMessage {
    fn partition_key(&self) -> Option<&[u8]> {
        self.headers().get(crate::PARTITION_KEY_HEADER)
    }
}

impl IncomingMessage for RedisTestMessage {
    fn payload(&self) -> &[u8] {
        self.delivery
            .as_ref()
            .map(|d| d.payload.as_ref())
            .unwrap_or_default()
    }

    fn headers(&self) -> &HeaderMap {
        static EMPTY: OnceLock<HeaderMap> = OnceLock::new();
        self.delivery
            .as_ref()
            .map_or_else(|| EMPTY.get_or_init(HeaderMap::new), |d| &d.headers)
    }

    // The stub broker mirrors the real one here too, so a keyed-lane test in-process behaves the
    // way the same service will against a live Redis.
    fn partition_key(&self) -> Option<&[u8]> {
        Partitioned::partition_key(self)
    }

    fn ack(mut self) -> impl Future<Output = Result<(), AckError>> {
        self.delivery.take();
        ready(Ok(()))
    }

    fn nack(mut self, requeue: bool) -> impl Future<Output = Result<(), AckError>> {
        let delivery = self
            .delivery
            .take()
            .expect("RedisTestMessage ack/nack invoked twice");
        if requeue {
            // The requeue bypasses `KeyRouter::publish`, so count the re-enqueue here to balance
            // this message's `Drop` decrement. The redelivered copy is consumed (and decremented) in
            // turn.
            if self.requeue.send(delivery).is_ok()
                && let Some(coordinator) = &self.coordinator
            {
                coordinator.enqueued();
            }
        }
        ready(Ok(()))
    }
}

impl BatchSubscriber for RedisTestSubscriber {
    type Batch = Vec<RedisTestMessage>;

    /// Drains whatever is already buffered in the subscriber's channel, at least one message and
    /// at most `size`. Blocks until the first message arrives.
    fn batches(
        &mut self,
        size: NonZeroUsize,
    ) -> impl Stream<Item = Result<Self::Batch, Self::Error>> + Send + '_ {
        let requeue = self.requeue.clone();
        let coordinator = self.coordinator.clone();
        futures::stream::poll_fn(move |cx| {
            let first = match self.rx.poll_recv(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Ready(Some(d)) => {
                    RedisTestMessage::from_delivery(d, requeue.clone(), coordinator.clone())
                }
            };
            let mut batch = vec![first];
            while batch.len() < size.get() {
                match self.rx.poll_recv(cx) {
                    Poll::Ready(Some(d)) => {
                        batch.push(RedisTestMessage::from_delivery(
                            d,
                            requeue.clone(),
                            coordinator.clone(),
                        ));
                    }
                    Poll::Ready(None) | Poll::Pending => break,
                }
            }
            Poll::Ready(Some(Ok(batch)))
        })
    }
}
