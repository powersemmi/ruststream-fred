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

use fred::clients::Pool;
use futures::Stream;
use ruststream::runtime::RETRY_COUNT_HEADER;
use ruststream::{
    AckError, BatchSubscriber, HeaderMap, IncomingMessage, Partitioned, Subscriber,
    testing::Coordinator,
};
use tokio::sync::mpsc;

use crate::pipeline::{RoundMessage, TestForm, Window};
use crate::{
    delay::next_retry_count,
    error::RedisError,
    message::{DELIVERY_COUNT_HEADER, IDLE_MS_HEADER},
    testing::{
        broker::{Settlement, StreamRetry, TestBrokerState},
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

/// What a subscription's retry does with the entry.
#[derive(Clone, Debug)]
struct Retry {
    /// The subscription's own queue: where an entry put back on the stream arrives, as a fresh
    /// one. This is how the fresh tail and a reliable list redeliver.
    requeue: DeliverySender,
    /// Set on a claiming subscription, which retries through its pending entries list instead.
    claim: Option<Claimback>,
}

/// Where a claiming subscription's retried entry waits, and for how long.
#[derive(Clone, Debug)]
struct Claimback {
    min_idle: Duration,
    claims: DeliverySender,
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
    /// Whether the descriptor named a delay queue, which is what makes a delay native here as it
    /// is against a real server.
    delayed: bool,
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
        retry: StreamRetry,
    ) -> Self {
        // The harness installs its coordinator before any subscription opens, so reading it here
        // captures the live coordinator for the whole subscription.
        let coordinator = state.coordinator();
        let claiming = retry.min_idle().map(|min_idle| {
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
            delayed: retry.is_delayed(),
        }
    }

    /// What a retry on this subscription does, handed to every delivery.
    fn retry(&self) -> Retry {
        Retry {
            requeue: self.requeue.clone(),
            claim: self.claiming.as_ref().map(|claiming| Claimback {
                min_idle: claiming.min_idle,
                claims: claiming.tx.clone(),
            }),
        }
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

impl RedisTestSubscriber {
    /// Yields one delivery at a time, each with a slot in `window`, telling the window how many
    /// more have arrived behind it, and reports a failed flush of the window once.
    pub(crate) fn round_stream<'a>(
        &'a mut self,
        window: &'a Arc<Window<TestForm>>,
    ) -> impl Stream<Item = Result<RoundMessage<TestForm>, RedisError>> + Send + 'a {
        let retry = self.retry();
        let coordinator = self.coordinator.clone();
        let settlement = self.settlement;
        let delayed = self.delayed;
        let pool = self.state.pool().clone();
        futures::stream::poll_fn(move |cx| {
            if let Some(failure) = window.take_failure() {
                return Poll::Ready(Some(Err(failure)));
            }
            self.poll_delivery(cx).map(|next| {
                next.map(|delivery| {
                    let waiting = self.rx.len()
                        + self
                            .claiming
                            .as_ref()
                            .map_or(0, |claiming| claiming.rx.len());
                    window.yielded(waiting);
                    let delivery = RedisTestMessage::from_delivery(
                        delivery,
                        retry.clone(),
                        coordinator.clone(),
                        settlement,
                        delayed,
                        pool.clone(),
                    );
                    Ok(RoundMessage::new(
                        delivery,
                        Arc::clone(window),
                        window.open(),
                    ))
                })
            })
        })
    }
}

impl RedisTestSubscriber {
    /// Yields batches of up to `size` deliveries already waiting, each batch sharing one slot of
    /// `window`, as the subscription's own batches are assembled.
    pub(crate) fn round_batches<'a>(
        &'a mut self,
        window: &'a Arc<Window<TestForm>>,
        size: NonZeroUsize,
    ) -> impl Stream<Item = Result<Vec<RoundMessage<TestForm>>, RedisError>> + Send + 'a {
        window.fill_at(size.get());
        let retry = self.retry();
        let coordinator = self.coordinator.clone();
        let settlement = self.settlement;
        let delayed = self.delayed;
        let pool = self.state.pool().clone();
        futures::stream::poll_fn(move |cx| {
            if let Some(failure) = window.take_failure() {
                return Poll::Ready(Some(Err(failure)));
            }
            let first = match self.poll_delivery(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Ready(Some(delivery)) => delivery,
            };
            let mut deliveries = vec![first];
            while deliveries.len() < size.get() {
                match self.poll_delivery(cx) {
                    Poll::Ready(Some(delivery)) => deliveries.push(delivery),
                    Poll::Ready(None) | Poll::Pending => break,
                }
            }
            let members = u32::try_from(deliveries.len()).unwrap_or(u32::MAX);
            let waiting = self.rx.len()
                + self
                    .claiming
                    .as_ref()
                    .map_or(0, |claiming| claiming.rx.len());
            for _ in 0..members {
                window.yielded(waiting);
            }
            let round = window.open_batch(members);
            let batch = deliveries
                .into_iter()
                .map(|delivery| {
                    let delivery = RedisTestMessage::from_delivery(
                        delivery,
                        retry.clone(),
                        coordinator.clone(),
                        settlement,
                        delayed,
                        pool.clone(),
                    );
                    RoundMessage::new(delivery, Arc::clone(window), round.clone())
                })
                .collect();
            Poll::Ready(Some(Ok(batch)))
        })
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
        let retry = self.retry();
        let coordinator = self.coordinator.clone();
        let settlement = self.settlement;
        let delayed = self.delayed;
        let pool = self.state.pool().clone();
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
                        delayed,
                        pool.clone(),
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
    /// Whether the subscription named a delay queue, so a delay is served exactly.
    delayed: bool,
    /// The server's own delivery count, counting this delivery, on a claiming subscription.
    delivered: Option<u64>,
    /// The stand-in's pool, which `Ctx<keys::FredPool>` hands the handler.
    pool: Pool,
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
        delayed: bool,
        pool: Pool,
    ) -> Self {
        // Only a claiming subscription stamps the counter, and only there does the real broker
        // report a count of its own.
        let delivered = retry
            .claim
            .is_some()
            .then(|| next_delivery_count(&delivery.headers));
        Self {
            delivery: Some(delivery),
            retry,
            coordinator,
            settlement,
            delayed,
            delivered,
            pool,
        }
    }

    /// The stand-in's pool, for the per-delivery context.
    pub(crate) const fn pool(&self) -> &Pool {
        &self.pool
    }

    /// What settling this delivery answers, before the settle runs: nothing on a form that settles,
    /// `Unsupported` on Pub/Sub and a simple list, as on a server.
    pub(crate) fn settle_answer(&self) -> Result<(), AckError> {
        match self.settlement {
            Settlement::Settleable => Ok(()),
            Settlement::Unsupported => Err(AckError::Unsupported),
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

    /// The count a claiming subscription reports, the way the real one reports the server's.
    fn redelivery_count(&self) -> Option<u64> {
        self.delivered
    }

    fn nack(mut self, requeue: bool) -> impl Future<Output = Result<(), AckError>> {
        let delivery = self
            .delivery
            .take()
            .expect("RedisTestMessage ack/nack invoked twice");
        // A transport that cannot acknowledge cannot redeliver either: the delivery is dropped and
        // the caller told, rather than quietly re-queued into a subscription the real one would not.
        if requeue && self.settlement == Settlement::Settleable {
            match self.retry.claim.clone() {
                Some(claim) => self.hold(delivery, claim.min_idle, claim.claims, Hold::Claimed),
                // The requeue bypasses `KeyRouter::publish`, so count the re-enqueue here to
                // balance this message's `Drop` decrement. The redelivered copy is consumed (and
                // decremented) in turn.
                None => {
                    if self.retry.requeue.send(delivery).is_ok()
                        && let Some(coordinator) = &self.coordinator
                    {
                        coordinator.enqueued();
                    }
                }
            }
        }
        ready(self.settlement_result())
    }

    /// The answer the real delivery gives: a named delay queue or a claiming read mode holds the
    /// message back itself, anything else leaves the delay to the runtime's copy.
    fn supports_nack_after(&self) -> bool {
        self.settlement == Settlement::Settleable && (self.delayed || self.retry.claim.is_some())
    }

    /// Holds the delivery back the way the subscription's own mechanism holds it: a delay queue
    /// serves `delay` exactly and the entry comes back fresh, while a claiming subscription
    /// without one leaves it pending and claims it back on its `min_idle`.
    ///
    /// # Errors
    ///
    /// Returns [`AckError::Unsupported`] on a subscription with neither, the way the real
    /// delivery reports it.
    fn nack_after(mut self, delay: Duration) -> impl Future<Output = Result<(), AckError>> {
        if !self.supports_nack_after() {
            return ready(Err(AckError::Unsupported));
        }
        let delivery = self
            .delivery
            .take()
            .expect("RedisTestMessage ack/nack invoked twice");
        if self.delayed {
            // The delay queue re-adds the entry to the stream, so it arrives as a fresh one with
            // no claim counters of its own.
            self.hold(delivery, delay, self.retry.requeue.clone(), Hold::Queued);
        } else if let Some(claim) = self.retry.claim.clone() {
            self.hold(delivery, claim.min_idle, claim.claims, Hold::Claimed);
        }
        ready(Ok(()))
    }
}

impl RedisTestMessage {
    /// Re-enqueues `delivery` on `queue` after `wait`, stamping the counter the mechanism named by
    /// `hold` raises on a real server.
    ///
    /// The wait is off the reaction the harness drives, exactly as a broker's own delayed
    /// redelivery is: the delivery is consumed now and re-enqueued when the timer fires, which
    /// [`TestApp::advance`](ruststream::testing::TestApp::advance) is what moves.
    fn hold(&self, mut delivery: Delivery, wait: Duration, queue: DeliverySender, hold: Hold) {
        match hold {
            Hold::Claimed => {
                let claimed = next_delivery_count(&delivery.headers);
                delivery
                    .headers
                    .insert(DELIVERY_COUNT_HEADER, claimed.to_string());
                delivery.headers.insert(
                    IDLE_MS_HEADER,
                    u64::try_from(wait.as_millis())
                        .unwrap_or(u64::MAX)
                        .to_string(),
                );
            }
            Hold::Queued => {
                delivery.headers.insert(
                    RETRY_COUNT_HEADER,
                    next_retry_count(&delivery.headers).to_string(),
                );
            }
        }
        match self.coordinator.clone() {
            Some(coordinator) => {
                let counted = coordinator.clone();
                coordinator.schedule_redelivery(wait, move || {
                    if queue.send(delivery).is_ok() {
                        counted.enqueued();
                    }
                });
            }
            // Outside a harness run nothing drives the clock, so the wait is an ordinary timer.
            None => {
                tokio::spawn(async move {
                    tokio::time::sleep(wait).await;
                    let _ = queue.send(delivery);
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
        let delayed = self.delayed;
        let pool = self.state.pool().clone();
        futures::stream::poll_fn(move |cx| {
            let first = match self.poll_delivery(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Ready(Some(d)) => RedisTestMessage::from_delivery(
                    d,
                    retry.clone(),
                    coordinator.clone(),
                    settlement,
                    delayed,
                    pool.clone(),
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
                            delayed,
                            pool.clone(),
                        ));
                    }
                    Poll::Ready(None) | Poll::Pending => break,
                }
            }
            Poll::Ready(Some(Ok(batch)))
        })
    }
}

/// Which mechanism holds a delivery back, and so which counter the held entry comes back with.
///
/// The two are exclusive on a real server and count different things, which is why a cap reads one
/// or the other and never both.
#[derive(Debug, Clone, Copy)]
enum Hold {
    /// The pending entries list of a claiming subscription: nothing is appended to the stream and
    /// nothing is acknowledged, and the entry is claimed back once it has been idle `min_idle`,
    /// with the server's delivery count one higher.
    Claimed,
    /// The ZSET delay queue: the entry is re-added to the stream when it comes due, carrying the
    /// framework's retry count one higher, which is the only count a subscription reading the
    /// fresh tail has.
    Queued,
}

/// The delivery count a claimed entry carries next: the one it reports now, plus this claim.
fn next_delivery_count(headers: &HeaderMap) -> u64 {
    headers
        .get_str(DELIVERY_COUNT_HEADER)
        .and_then(|raw| raw.parse::<u64>().ok())
        .unwrap_or(0)
        + 1
}
