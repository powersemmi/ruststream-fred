//! The window of one subscription: the segments its deliveries' handlers queued, the settles its
//! deliveries owe, and the flush that sends both.
//!
//! A delivery takes a slot when it is yielded and gives it back when it settles. What its handler
//! queues goes into the slot's segment, a `fred` pipeline created on the first queued command. The
//! settle decides what happens to the segment: `ack` commits it, every other outcome drops it. A
//! committed segment and the settle commands wait in the window until it flushes: once the read's
//! `COUNT` has settled, once nothing is outstanding, or when the subscription stops.
//!
//! The segments of one flush are sent concurrently on the window's own client, in commit order,
//! followed by one pipeline of settles; `fred` writes them to the socket back to back.

use std::fmt::{Debug, Formatter};
use std::sync::{Arc, Mutex, MutexGuard};

use fred::clients::{Client, Pipeline, WithOptions};
use fred::error::Error;
use fred::interfaces::ClientLike;
use fred::types::config::Options;
use fred::types::{ClusterHash, CustomCommand, Value};
use futures::future::join_all;
use ruststream::{AckError, IncomingMessage};

use super::{pipeline_on, queued};
use crate::error::RedisError;

/// One delivery's buffer: a `fred` pipeline, pinned to the subscription's slot and opened with
/// `MULTI` under `.atomic()`.
#[derive(Clone)]
pub enum Segment {
    Plain(Pipeline<Client>),
    Atomic(WithOptions<Pipeline<Client>>),
}

impl Debug for Segment {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Plain(_) => "Segment::Plain",
            Self::Atomic(_) => "Segment::Atomic",
        })
    }
}

impl Segment {
    fn pipeline(&self) -> &Pipeline<Client> {
        match self {
            Self::Plain(pipeline) => pipeline,
            Self::Atomic(pinned) => pinned,
        }
    }
}

/// `MULTI` and `EXEC` are sent as plain commands inside the window's pipeline: `fred`'s own
/// transaction waits for its reply before the client sends anything else, which would cost the
/// window a round trip per segment.
fn transaction_edge(name: &'static str, slot: u16) -> CustomCommand {
    CustomCommand::new_static(name, ClusterHash::Custom(slot), false)
}

/// What the slots of one subscription hold: the part of the window a handler's facade reaches.
pub(crate) struct Segments {
    client: Client,
    atomic: bool,
    /// The cluster slot of the subscription key, which an atomic segment is pinned to.
    hash_slot: u16,
    slots: Mutex<Slots>,
}

#[derive(Default)]
struct Slots {
    segments: Vec<Option<Segment>>,
    free: Vec<u32>,
}

impl Segments {
    fn lock(&self) -> MutexGuard<'_, Slots> {
        self.slots.lock().expect("pipeline slots poisoned")
    }

    fn open(&self) -> u32 {
        let mut slots = self.lock();
        if let Some(slot) = slots.free.pop() {
            return slot;
        }
        slots.segments.push(None);
        u32::try_from(slots.segments.len() - 1).expect("fewer than 2^32 deliveries in flight")
    }

    /// Takes the slot's segment and gives the slot back.
    fn close(&self, slot: u32) -> Option<Segment> {
        let mut slots = self.lock();
        let segment = slots.segments.get_mut(slot as usize).and_then(Option::take);
        slots.free.push(slot);
        segment
    }

    fn segment(&self, slot: u32) -> Result<Segment, Error> {
        let mut slots = self.lock();
        let entry = slots
            .segments
            .get_mut(slot as usize)
            .expect("a round names a slot of its own window");
        if let Some(segment) = entry {
            let segment = segment.clone();
            drop(slots);
            return Ok(segment);
        }
        let pipeline = pipeline_on(&self.client);
        let segment = if self.atomic {
            // Pinned to the subscription key's slot, and told not to follow a redirection: a
            // command for another slot then fails inside the transaction, and Redis refuses the
            // whole `EXEC` rather than running part of it.
            let pinned = pipeline.with_options(&Options {
                cluster_hash: Some(ClusterHash::Custom(self.hash_slot)),
                max_redirections: Some(0),
                ..Options::default()
            });
            queued(
                pinned.custom::<(), Value>(transaction_edge("MULTI", self.hash_slot), Vec::new()),
            )?;
            Segment::Atomic(pinned)
        } else {
            Segment::Plain(pipeline)
        };
        *entry = Some(segment.clone());
        drop(slots);
        Ok(segment)
    }
}

/// A delivery's handle on its slot: what the facade and the settle reach the window through.
#[derive(Clone)]
pub(crate) struct Round {
    segments: Arc<Segments>,
    slot: u32,
}

impl Round {
    pub(crate) fn segment(&self) -> Result<Segment, Error> {
        self.segments.segment(self.slot)
    }
}

/// How one subscription form settles inside a window.
///
/// Public in name only, for the bound of [`RoundMessage`](super::RoundMessage): the module that
/// holds it is private.
pub trait Form: Send + Sync + 'static {
    /// The form's own delivery.
    type Message: IncomingMessage + Send + Sync + 'static;
    /// The broker's connection pool, which a delivery's context hands out.
    fn pool(msg: &Self::Message) -> &fred::clients::Pool;
    /// One settle command, owed until the window flushes.
    type Op: Send + 'static;

    /// The settle an acknowledgement owes.
    ///
    /// # Errors
    ///
    /// [`AckError::Unsupported`] on a form that settles nothing.
    fn ack(&self, msg: Self::Message, ops: &mut Vec<Self::Op>) -> Result<(), AckError>;

    /// The settle a rejection owes: the requeue's own commands when `requeue`, the plain settle
    /// otherwise.
    ///
    /// # Errors
    ///
    /// [`AckError::Unsupported`] on a form that settles nothing.
    fn nack(
        &self,
        msg: Self::Message,
        requeue: bool,
        ops: &mut Vec<Self::Op>,
    ) -> Result<(), AckError>;

    /// The settle a delayed redelivery owes.
    ///
    /// # Errors
    ///
    /// [`AckError::Unsupported`] where the form has no delay of its own.
    fn nack_after(
        &self,
        msg: Self::Message,
        delay: std::time::Duration,
        ops: &mut Vec<Self::Op>,
    ) -> Result<(), AckError>;

    /// Closes an atomic segment before `EXEC`: queues the delivery's settle into it where the
    /// form can, and leaves in `ops` what has to run after the segments instead.
    ///
    /// # Errors
    ///
    /// `fred`'s error when a command cannot be queued.
    fn close_atomic(&self, segment: &Segment, ops: &mut Vec<Self::Op>) -> Result<(), Error>;

    /// Sends the settles of one flush, after its segments.
    fn send_ops(
        &self,
        client: &Client,
        ops: &mut Vec<Self::Op>,
    ) -> impl Future<Output = Sent> + Send;
}

/// What one part of a flush carried and how much of it failed.
#[derive(Debug, Default)]
pub struct Sent {
    pub(crate) commands: usize,
    pub(crate) failed: usize,
    pub(crate) first_error: Option<Error>,
}

impl Sent {
    pub(crate) fn record(&mut self, results: Vec<Result<Value, Error>>) {
        self.commands += results.len();
        for result in results {
            if let Err(err) = result {
                self.failed += 1;
                self.first_error.get_or_insert(err);
            }
        }
    }

    fn merge(&mut self, other: Self) {
        self.commands += other.commands;
        self.failed += other.failed;
        if self.first_error.is_none() {
            self.first_error = other.first_error;
        }
    }
}

/// What a window owes between two flushes.
struct Owed<Op> {
    /// Deliveries yielded or buffered and not settled yet.
    outstanding: usize,
    /// Deliveries settled since the last flush.
    settled: usize,
    /// Deliveries that arrived and were not yielded yet, on a form whose reads are not counted
    /// in `outstanding`.
    waiting: usize,
    committed: Vec<Segment>,
    ops: Vec<Op>,
    /// The ops of the delivery being settled, for an atomic segment to take.
    scratch: Vec<Op>,
    /// A flush that failed, reported once on the delivery stream.
    failure: Option<RedisError>,
}

/// The window of one pipelined subscription.
pub(crate) struct Window<F: Form> {
    form: F,
    segments: Arc<Segments>,
    /// The subscription, named in a flush failure.
    name: Arc<str>,
    /// The read's `COUNT`: this many settles fill the window.
    capacity: usize,
    owed: Mutex<Owed<F::Op>>,
}

impl<F: Form> Debug for Window<F> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Window")
            .field("name", &self.name)
            .field("capacity", &self.capacity)
            .finish_non_exhaustive()
    }
}

/// One flush, taken out of the window so it is sent without holding the lock.
struct Flush<Op> {
    committed: Vec<Segment>,
    ops: Vec<Op>,
}

impl<F: Form> Window<F> {
    pub(crate) fn new(
        form: F,
        client: Client,
        atomic: bool,
        name: impl Into<Arc<str>>,
        capacity: usize,
    ) -> Self {
        let name = name.into();
        let hash_slot = fred::util::redis_keyslot(name.as_bytes());
        Self {
            form,
            segments: Arc::new(Segments {
                client,
                atomic,
                hash_slot,
                slots: Mutex::new(Slots::default()),
            }),
            name,
            capacity: capacity.max(1),
            owed: Mutex::new(Owed {
                outstanding: 0,
                settled: 0,
                waiting: 0,
                committed: Vec::new(),
                ops: Vec::new(),
                scratch: Vec::new(),
                failure: None,
            }),
        }
    }

    fn owed(&self) -> MutexGuard<'_, Owed<F::Op>> {
        self.owed.lock().expect("pipeline window poisoned")
    }

    /// Counts `n` deliveries a read has just buffered.
    pub(crate) fn fetched(&self, n: usize) {
        self.owed().outstanding += n;
    }

    /// Forgets `n` buffered deliveries a seek discarded before they were yielded.
    pub(crate) fn discarded(&self, n: usize) {
        let mut owed = self.owed();
        owed.outstanding = owed.outstanding.saturating_sub(n);
    }

    /// Counts one delivery being yielded, on a form that reads one at a time, and how many more
    /// have arrived behind it.
    pub(crate) fn yielded(&self, waiting: usize) {
        let mut owed = self.owed();
        owed.outstanding += 1;
        owed.waiting = waiting;
    }

    /// A slot for a delivery being yielded.
    pub(crate) fn open(&self) -> Round {
        Round {
            segments: Arc::clone(&self.segments),
            slot: self.segments.open(),
        }
    }

    /// The failure of an earlier flush, once.
    pub(crate) fn take_failure(&self) -> Option<RedisError> {
        self.owed().failure.take()
    }

    /// Settles one delivery: `settle` records what the outcome owes, and `commit` says whether
    /// its segment is kept. Flushes when the window is full or nothing is outstanding.
    pub(crate) async fn settle(
        &self,
        round: &Round,
        commit: bool,
        settle: impl FnOnce(&F, &mut Vec<F::Op>) -> Result<(), AckError>,
    ) -> Result<(), AckError> {
        let segment = self.segments.close(round.slot);
        let flush = {
            let mut guard = self.owed();
            let owed = &mut *guard;
            let result = match segment {
                Some(segment) if commit => {
                    let mut scratch = std::mem::take(&mut owed.scratch);
                    let mut result = settle(&self.form, &mut scratch);
                    // A form that settles nothing answers `Unsupported` to an `ack`, and its
                    // segment is committed all the same: what the handler queued still follows
                    // the outcome.
                    let mut keep = matches!(result, Ok(()) | Err(AckError::Unsupported));
                    if keep
                        && self.segments.atomic
                        && let Err(err) = self.close_atomic(&segment, &mut scratch)
                    {
                        keep = false;
                        result = Err(AckError::Broker(Box::new(err)));
                    }
                    if keep {
                        owed.committed.push(segment);
                    }
                    owed.ops.append(&mut scratch);
                    owed.scratch = scratch;
                    result
                }
                // An outcome other than `ack` drops the segment: what the handler queued never
                // leaves.
                _ => settle(&self.form, &mut owed.ops),
            };
            owed.outstanding = owed.outstanding.saturating_sub(1);
            owed.settled += 1;
            let due = (owed.outstanding == 0 && owed.waiting == 0) || owed.settled >= self.capacity;
            let flush = due.then(|| Self::take(owed));
            drop(guard);
            (flush, result)
        };
        let (flush, result) = flush;
        if let Some(flush) = flush {
            self.send(flush).await;
        }
        result
    }

    /// Gives a delivery's slot back without settling it: the delivery was dropped unsettled, so
    /// its segment is dropped and the entry stays with the broker.
    pub(crate) fn abandon(&self, round: &Round) {
        drop(self.segments.close(round.slot));
        let mut owed = self.owed();
        owed.outstanding = owed.outstanding.saturating_sub(1);
    }

    /// Takes what the window owes, for a flush on stop.
    pub(crate) fn drain(&self) -> impl Future<Output = ()> + Send + '_ {
        let flush = Self::take(&mut self.owed());
        async move { self.send(flush).await }
    }

    fn close_atomic(&self, segment: &Segment, ops: &mut Vec<F::Op>) -> Result<(), Error> {
        self.form.close_atomic(segment, ops)?;
        queued(segment.pipeline().custom::<(), Value>(
            transaction_edge("EXEC", self.segments.hash_slot),
            Vec::new(),
        ))
    }

    fn take(owed: &mut Owed<F::Op>) -> Flush<F::Op> {
        owed.settled = 0;
        Flush {
            committed: std::mem::take(&mut owed.committed),
            ops: std::mem::take(&mut owed.ops),
        }
    }

    async fn send(&self, mut flush: Flush<F::Op>) {
        if flush.committed.is_empty() && flush.ops.is_empty() {
            self.give_back(flush);
            return;
        }
        // The segments are submitted first, in commit order, and the settles after them: the
        // futures of `join` are polled in order, and a pipeline reaches the client's queue on its
        // first poll, so every command a handler queued before its `ack` runs before that `ack`.
        let segments = join_all(
            flush
                .committed
                .iter()
                .map(|segment| segment.pipeline().try_all::<Value>()),
        );
        let settles = self.form.send_ops(&self.segments.client, &mut flush.ops);
        let (segments, settles) = futures::join!(segments, settles);
        let mut sent = Sent::default();
        for results in segments {
            sent.record(results);
        }
        sent.merge(settles);
        if let Some(error) = sent.first_error {
            let failure = RedisError::Flush(format!(
                "the window of `{}` sent {} commands and {} of them failed: {error}",
                self.name, sent.commands, sent.failed,
            ));
            self.owed().failure.get_or_insert(failure);
        }
        self.give_back(flush);
    }

    /// Returns a flush's buffers to the window, so the next flush reuses what they grew to.
    fn give_back(&self, mut flush: Flush<F::Op>) {
        flush.committed.clear();
        flush.ops.clear();
        let mut owed = self.owed();
        if owed.committed.capacity() < flush.committed.capacity() && owed.committed.is_empty() {
            owed.committed = flush.committed;
        }
        if owed.ops.capacity() < flush.ops.capacity() && owed.ops.is_empty() {
            owed.ops = flush.ops;
        }
    }
}
