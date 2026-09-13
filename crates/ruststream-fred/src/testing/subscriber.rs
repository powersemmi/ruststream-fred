//! [`RedisTestSubscriber`] and [`RedisTestMessage`].
//!
//! The subscriber wraps a [`router::DeliveryReceiver`] and yields one [`RedisTestMessage`] per
//! delivery. Dropping the subscriber unregisters its subscription from the underlying
//! [`router::KeyRouter`], so handlers stop receiving messages as soon as their task finishes.

use std::future::{Future, ready};
use std::num::NonZeroUsize;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};
use std::time::Duration;

use futures::Stream;
use ruststream::{
    AckError, BatchSubscriber, HeaderMap, IncomingMessage, Partitioned, Subscriber,
    testing::Coordinator,
};
use tokio::sync::mpsc;

use crate::{
    deadletter::{DELIVERY_COUNT_HEADER, IDLE_MS_HEADER},
    error::RedisError,
    testing::{
        broker::{Settlement, TestBrokerState},
        router::{Delivery, DeliveryReceiver, DeliverySender, SubscriptionId},
    },
};

/// The pending-entries machinery of a [`RedisStream::claiming`](crate::RedisStream::claiming)
/// subscription, absent on every other form.
///
/// A retried entry is not re-queued: it waits `min_idle` and then arrives on this channel, which
/// the subscription drains before its fresh one, the order `XREADGROUP ... CLAIM` returns entries
/// in.
struct Claiming {
    min_idle: Duration,
    tx: DeliverySender,
    rx: DeliveryReceiver,
}

/// What a subscription's `nack(requeue = true)` does with the entry.
#[derive(Clone, Debug)]
enum Retry {
    /// Straight back onto the subscription's own queue, as the fresh tail and a reliable list
    /// redeliver.
    Requeue(DeliverySender),
    /// Left pending: the claiming mode claims it back once it has been idle `min_idle`, with the
    /// delivery count one higher.
    Claim {
        min_idle: Duration,
        claims: DeliverySender,
    },
}

/// Subscriber returned by [`crate::testing::ConnectedRedisTestBroker::subscribe`].
pub struct RedisTestSubscriber {
    state: Arc<TestBrokerState>,
    id: SubscriptionId,
    rx: DeliveryReceiver,
    requeue: DeliverySender,
    /// A clone of the broker's harness coordinator, threaded into each yielded message so a requeue
    /// re-counts and a consumed delivery decrements. `None` outside a harness run.
    coordinator: Option<Coordinator>,
    /// Whether this subscription's form can settle, carried into every delivery it yields.
    settlement: Settlement,
    /// Set on a claiming subscription, which keeps a pending entries list of its own.
    claiming: Option<Claiming>,
}

impl std::fmt::Debug for RedisTestSubscriber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisTestSubscriber")
            .field("settlement", &self.settlement)
            .finish_non_exhaustive()
    }
}

impl RedisTestSubscriber {
    pub(crate) fn new(
        state: Arc<TestBrokerState>,
        id: SubscriptionId,
        rx: DeliveryReceiver,
        requeue: DeliverySender,
        settlement: Settlement,
        min_idle: Option<Duration>,
    ) -> Self {
        // The harness installs its coordinator before any subscription opens, so reading it here
        // captures the live coordinator for the whole subscription.
        let coordinator = state.coordinator();
        let claiming = min_idle.map(|min_idle| {
            let (tx, rx) = mpsc::unbounded_channel();
            Claiming { min_idle, tx, rx }
        });
        Self {
            state,
            id,
            rx,
            requeue,
            coordinator,
            settlement,
            claiming,
        }
    }

    /// What a `nack(requeue = true)` on this subscription does, handed to every delivery.
    fn retry(&self) -> Retry {
        self.claiming.as_ref().map_or_else(
            || Retry::Requeue(self.requeue.clone()),
            |claiming| Retry::Claim {
                min_idle: claiming.min_idle,
                claims: claiming.tx.clone(),
            },
        )
    }

    /// The next delivery for this subscription: a claimed entry before a fresh one, the order the
    /// server returns them in.
    fn poll_delivery(&mut self, cx: &mut Context<'_>) -> Poll<Option<Delivery>> {
        if let Some(claiming) = self.claiming.as_mut()
            && let Poll::Ready(Some(delivery)) = claiming.rx.poll_recv(cx)
        {
            return Poll::Ready(Some(delivery));
        }
        let claims = self.claiming.is_some();
        self.rx.poll_recv(cx).map(|next| {
            next.map(|mut delivery| {
                if claims {
                    stamp_fresh(&mut delivery);
                }
                delivery
            })
        })
    }
}

/// Stamps the two counters a claiming read reports onto a fresh entry: it has never been claimed,
/// so it has been idle no time and has been delivered no times before.
fn stamp_fresh(delivery: &mut Delivery) {
    delivery.headers.insert(DELIVERY_COUNT_HEADER, "0");
    delivery.headers.insert(IDLE_MS_HEADER, "0");
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
        let retry = self.retry();
        let coordinator = self.coordinator.clone();
        let settlement = self.settlement;
        // Poll the receiver in place rather than wrapping it in an owning stream, so `stream` can
        // be called again after the returned stream is dropped (the runtime and the conformance
        // helpers re-enter it per call).
        futures::stream::poll_fn(move |cx| {
            self.poll_delivery(cx).map(|next| {
                next.map(|delivery| {
                    Ok(RedisTestMessage::from_delivery(
                        delivery,
                        retry.clone(),
                        coordinator.clone(),
                        settlement,
                    ))
                })
            })
        })
    }
}

/// Message handed to handlers from a [`RedisTestSubscriber`].
///
/// On a settleable subscription (a stream, a reliable list) `ack` consumes the handle silently,
/// `nack(requeue = true)` re-queues the delivery on the owning subscription's channel so the next
/// handler invocation sees it again (matching the republish model the real broker uses), and
/// `nack(requeue = false)` drops it.
///
/// On the forms whose real deliveries cannot settle (Pub/Sub, a simple list) both report
/// [`AckError::Unsupported`] and nothing is re-queued, exactly as against a real server.
pub struct RedisTestMessage {
    delivery: Option<Delivery>,
    retry: Retry,
    /// A clone of the broker's harness coordinator. When set, this delivery is counted in flight and
    /// decremented exactly once when the message is consumed or dropped (see the `Drop` impl). `None`
    /// outside a harness run.
    coordinator: Option<Coordinator>,
    /// Inherited from the subscription that yielded this delivery.
    settlement: Settlement,
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
    fn from_delivery(
        delivery: Delivery,
        retry: Retry,
        coordinator: Option<Coordinator>,
        settlement: Settlement,
    ) -> Self {
        Self {
            delivery: Some(delivery),
            retry,
            coordinator,
            settlement,
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
        ready(self.settlement_result())
    }

    fn nack(mut self, requeue: bool) -> impl Future<Output = Result<(), AckError>> {
        let delivery = self
            .delivery
            .take()
            .expect("RedisTestMessage ack/nack invoked twice");
        // A transport that cannot acknowledge cannot redeliver either: the delivery is dropped and
        // the caller told, rather than quietly re-queued into a subscription the real one would not.
        if requeue && self.settlement == Settlement::Settleable {
            match self.retry.clone() {
                // The requeue bypasses `KeyRouter::publish`, so count the re-enqueue here to
                // balance this message's `Drop` decrement. The redelivered copy is consumed (and
                // decremented) in turn.
                Retry::Requeue(tx) => {
                    if tx.send(delivery).is_ok()
                        && let Some(coordinator) = &self.coordinator
                    {
                        coordinator.enqueued();
                    }
                }
                Retry::Claim { min_idle, claims } => {
                    self.leave_pending(delivery, min_idle, claims);
                }
            }
        }
        ready(self.settlement_result())
    }
}

impl RedisTestMessage {
    /// Leaves the entry in the subscription's pending entries list, the way the claiming mode
    /// retries on a real server: nothing is appended to the stream and nothing is acknowledged,
    /// and the entry is claimed back once it has been idle `min_idle`, with the delivery count one
    /// higher.
    ///
    /// The wait is off the reaction the harness drives, exactly as a broker's own delayed
    /// redelivery is: the delivery is consumed now and re-enqueued when the timer fires, which
    /// [`TestApp::advance`](ruststream::testing::TestApp::advance) is what moves.
    fn leave_pending(&self, mut delivery: Delivery, min_idle: Duration, claims: DeliverySender) {
        let claimed = next_delivery_count(&delivery.headers);
        delivery
            .headers
            .insert(DELIVERY_COUNT_HEADER, claimed.to_string());
        delivery.headers.insert(
            IDLE_MS_HEADER,
            u64::try_from(min_idle.as_millis())
                .unwrap_or(u64::MAX)
                .to_string(),
        );
        match self.coordinator.clone() {
            Some(coordinator) => {
                let counted = coordinator.clone();
                coordinator.schedule_redelivery(min_idle, move || {
                    if claims.send(delivery).is_ok() {
                        counted.enqueued();
                    }
                });
            }
            // Outside a harness run nothing drives the clock, so the wait is an ordinary timer.
            None => {
                tokio::spawn(async move {
                    tokio::time::sleep(min_idle).await;
                    let _ = claims.send(delivery);
                });
            }
        }
    }

    /// What a settle call reports on this delivery's form.
    fn settlement_result(&self) -> Result<(), AckError> {
        match self.settlement {
            Settlement::Settleable => Ok(()),
            Settlement::Unsupported => Err(AckError::Unsupported),
        }
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
        let retry = self.retry();
        let coordinator = self.coordinator.clone();
        let settlement = self.settlement;
        futures::stream::poll_fn(move |cx| {
            let first = match self.poll_delivery(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Ready(Some(d)) => RedisTestMessage::from_delivery(
                    d,
                    retry.clone(),
                    coordinator.clone(),
                    settlement,
                ),
            };
            let mut batch = vec![first];
            while batch.len() < size.get() {
                match self.poll_delivery(cx) {
                    Poll::Ready(Some(d)) => {
                        batch.push(RedisTestMessage::from_delivery(
                            d,
                            retry.clone(),
                            coordinator.clone(),
                            settlement,
                        ));
                    }
                    Poll::Ready(None) | Poll::Pending => break,
                }
            }
            Poll::Ready(Some(Ok(batch)))
        })
    }
}

/// The delivery count a claimed entry carries next: the one it reports now, plus this claim.
fn next_delivery_count(headers: &HeaderMap) -> u64 {
    headers
        .get_str(DELIVERY_COUNT_HEADER)
        .and_then(|raw| raw.parse::<u64>().ok())
        .unwrap_or(0)
        + 1
}
