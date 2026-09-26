//! The delivery of a pipelined subscription: the form's own delivery, with a slot in the window.

use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use ruststream::{AckError, HeaderMap, IncomingMessage, Partitioned};

use super::window::{Form, Round, Window};

/// A delivery of a `.pipeline()` subscription.
///
/// It reads as the form's own delivery does, and settles into the window instead of on the
/// connection: `ack` commits what the handler queued together with the delivery's settle, and
/// every other outcome drops it. A delivery dropped unsettled drops its segment and stays with
/// the broker.
///
/// # Examples
///
/// ```
/// # mod demo {
/// use ruststream::prelude::*;
/// use ruststream::subscriber;
/// use ruststream_fred::PipelinedStream;
/// # #[derive(serde::Deserialize)]
/// # struct Order { id: u64 }
///
/// // The subscription yields this type; a handler reads it as any delivery.
/// #[subscriber(PipelinedStream::new("orders").group("workers"))]
/// async fn work(order: &Order) -> HandlerOutcome {
///     let _ = order.id;
///     HandlerOutcome::ack()
/// }
/// # }
/// ```
pub struct RoundMessage<F: Form> {
    /// `None` once settled.
    inner: Option<F::Message>,
    window: Arc<Window<F>>,
    round: Round,
    /// Set once the round is recorded against the task handling the delivery.
    entered: AtomicBool,
}

impl<F: Form> Debug for RoundMessage<F>
where
    F::Message: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RoundMessage")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl<F: Form> RoundMessage<F> {
    pub(crate) const fn new(inner: F::Message, window: Arc<Window<F>>, round: Round) -> Self {
        Self {
            inner: Some(inner),
            window,
            round,
            entered: AtomicBool::new(false),
        }
    }

    /// The form's own delivery.
    ///
    /// # Panics
    ///
    /// Panics once the delivery has settled, which consumes it; nothing reads it after that.
    pub(crate) fn inner(&self) -> &F::Message {
        self.inner
            .as_ref()
            .expect("a pipelined delivery is read only before it settles")
    }

    pub(crate) fn round(&self) -> &Round {
        &self.round
    }

    async fn settle(
        mut self,
        commit: bool,
        settle: impl FnOnce(&F, F::Message, &mut Vec<F::Op>) -> Result<(), AckError>,
    ) -> Result<(), AckError> {
        #[cfg_attr(not(feature = "testing"), allow(unused_mut))]
        let mut inner = self
            .inner
            .take()
            .expect("a pipelined delivery settles once");
        let window = Arc::clone(&self.window);
        #[cfg(feature = "testing")]
        window.hold(F::take_flight(&mut inner));
        window
            .settle(&self.round, commit, |form, ops| settle(form, inner, ops))
            .await
    }
}

impl<F: Form> Drop for RoundMessage<F> {
    fn drop(&mut self) {
        if self.inner.is_some() {
            self.window.abandon(&self.round);
        }
    }
}

impl<F: Form> Partitioned for RoundMessage<F>
where
    F::Message: Partitioned,
{
    fn partition_key(&self) -> Option<&[u8]> {
        Partitioned::partition_key(self.inner())
    }
}

impl<F: Form> RoundMessage<F> {
    /// Records the delivery's round against the task reading it, once: the runtime decodes a
    /// delivery in the task that handles it, before the handler runs, and a reply marked to join
    /// the round finds it there.
    fn enter(&self) {
        // The load keeps every read after the first to a plain one.
        if !self.entered.load(Ordering::Relaxed) && !self.entered.swap(true, Ordering::Relaxed) {
            self.round.enter();
        }
    }
}

impl<F: Form> IncomingMessage for RoundMessage<F> {
    /// Also enters the round: the payload is what the runtime reads to decode the input, and it
    /// reads the headers only when something asks for them.
    fn payload(&self) -> &[u8] {
        self.enter();
        self.inner().payload()
    }

    /// Also enters the round, for a handler that reads the headers alone.
    fn headers(&self) -> &HeaderMap {
        self.enter();
        self.inner().headers()
    }

    fn partition_key(&self) -> Option<&[u8]> {
        self.inner().partition_key()
    }

    fn redelivery_count(&self) -> Option<u64> {
        self.inner().redelivery_count()
    }

    fn supports_nack_after(&self) -> bool {
        self.inner().supports_nack_after()
    }

    async fn ack(self) -> Result<(), AckError> {
        self.settle(true, F::ack).await
    }

    async fn nack(self, requeue: bool) -> Result<(), AckError> {
        self.settle(false, move |form, msg, ops| form.nack(msg, requeue, ops))
            .await
    }

    async fn nack_after(self, delay: Duration) -> Result<(), AckError> {
        self.settle(false, move |form, msg, ops| {
            form.nack_after(msg, delay, ops)
        })
        .await
    }
}
