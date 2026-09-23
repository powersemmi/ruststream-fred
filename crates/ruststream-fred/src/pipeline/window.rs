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
use fred::interfaces::{
    ClientLike, KeysInterface, ListInterface, PubsubInterface, StreamsInterface,
};
use fred::types::config::Options;
use fred::types::{ClusterHash, CustomCommand, Value};
use futures::future::join_all;
use ruststream::{AckError, IncomingMessage};

use super::{Rounds, pipeline_on};
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
    rounds: Arc<Rounds>,
    atomic: bool,
    /// The cluster slot of the subscription key, which an atomic segment is pinned to.
    hash_slot: u16,
    slots: Mutex<Slots>,
}

#[derive(Default)]
struct Slots {
    segments: Vec<Option<Segment>>,
    /// The deliveries of each slot not settled yet: one for a delivery, the batch's size for a
    /// batch, whose segment is the batch.
    members: Vec<u32>,
    /// Whether every member settled so far acknowledged.
    clean: Vec<bool>,
    free: Vec<u32>,
}

/// What closing a slot's last member hands back: its segment and whether every member of it
/// acknowledged.
struct Closed {
    segment: Option<Segment>,
    clean: bool,
}

impl Segments {
    fn lock(&self) -> MutexGuard<'_, Slots> {
        self.slots.lock().expect("pipeline slots poisoned")
    }

    fn open(&self, members: u32) -> u32 {
        let mut slots = self.lock();
        let slot = slots.free.pop().unwrap_or_else(|| {
            slots.segments.push(None);
            slots.members.push(0);
            slots.clean.push(true);
            u32::try_from(slots.segments.len() - 1).expect("fewer than 2^32 deliveries in flight")
        });
        slots.members[slot as usize] = members;
        slots.clean[slot as usize] = true;
        drop(slots);
        slot
    }

    /// Settles one member of the slot. The last one takes the segment and gives the slot back;
    /// the others hand back nothing.
    fn close(&self, slot: u32, acknowledged: bool) -> Option<Closed> {
        let mut slots = self.lock();
        let at = slot as usize;
        slots.clean[at] &= acknowledged;
        slots.members[at] = slots.members[at].saturating_sub(1);
        if slots.members[at] > 0 {
            return None;
        }
        let closed = Closed {
            segment: slots.segments[at].take(),
            clean: slots.clean[at],
        };
        slots.free.push(slot);
        drop(slots);
        Some(closed)
    }

    /// The slot's segment, created on first use: a pipeline on the window's client, opened with
    /// `MULTI` under `.atomic()`.
    async fn segment(&self, slot: u32) -> Result<Segment, Error> {
        if let Some(segment) = self.existing(slot) {
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
            pinned
                .custom::<(), Value>(transaction_edge("MULTI", self.hash_slot), Vec::new())
                .await?;
            Segment::Atomic(pinned)
        } else {
            Segment::Plain(pipeline)
        };
        let mut slots = self.lock();
        let entry = slots
            .segments
            .get_mut(slot as usize)
            .expect("a round names a slot of its own window");
        // One handler queues into one slot, so a segment created here meanwhile is its own.
        let segment = entry.get_or_insert(segment).clone();
        drop(slots);
        Ok(segment)
    }

    fn existing(&self, slot: u32) -> Option<Segment> {
        let slots = self.lock();
        let segment = slots
            .segments
            .get(slot as usize)
            .expect("a round names a slot of its own window")
            .clone();
        drop(slots);
        segment
    }
}

/// A delivery's handle on its slot: what the facade and the settle reach the window through.
#[derive(Clone)]
pub(crate) struct Round {
    segments: Arc<Segments>,
    slot: u32,
}

impl Round {
    pub(crate) async fn segment(&self) -> Result<Segment, Error> {
        self.segments.segment(self.slot).await
    }

    /// Whether `other` is this very slot of this very window.
    pub(crate) fn same(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.segments, &other.segments) && self.slot == other.slot
    }

    /// Records that the delivery of this round is handled in the current task.
    pub(crate) fn enter(&self) {
        self.segments.rounds.enter(self);
    }

    /// Records that `publisher` publishes into this round from the current task.
    pub(crate) fn bind(&self, publisher: u64) {
        self.segments.rounds.bind(self, publisher);
    }

    /// Queues an `XADD` into this round's segment.
    pub(crate) async fn xadd(
        &self,
        key: &str,
        fields: Vec<(String, Vec<u8>)>,
    ) -> Result<(), Error> {
        match self.segment().await? {
            Segment::Plain(segment) => {
                segment
                    .xadd::<(), _, _, _, _>(key, false, None::<()>, "*", fields)
                    .await
            }
            Segment::Atomic(segment) => {
                segment
                    .xadd::<(), _, _, _, _>(key, false, None::<()>, "*", fields)
                    .await
            }
        }
    }

    /// Queues an `LPUSH`, and the key's expiry when there is one, into this round's segment.
    pub(crate) async fn lpush(
        &self,
        key: &str,
        body: Vec<u8>,
        ttl_millis: Option<i64>,
    ) -> Result<(), Error> {
        match self.segment().await? {
            Segment::Plain(segment) => {
                segment.lpush::<(), _, _>(key, body).await?;
                if let Some(ttl) = ttl_millis {
                    segment.pexpire::<(), _>(key, ttl, None).await?;
                }
            }
            Segment::Atomic(segment) => {
                segment.lpush::<(), _, _>(key, body).await?;
                if let Some(ttl) = ttl_millis {
                    segment.pexpire::<(), _>(key, ttl, None).await?;
                }
            }
        }
        Ok(())
    }

    /// Queues a `PUBLISH`, or an `SPUBLISH` when `sharded`, into this round's segment.
    pub(crate) async fn publish(
        &self,
        channel: &str,
        body: Vec<u8>,
        sharded: bool,
    ) -> Result<(), Error> {
        match (self.segment().await?, sharded) {
            (Segment::Plain(segment), false) => segment.publish::<(), _, _>(channel, body).await,
            (Segment::Plain(segment), true) => segment.spublish::<(), _, _>(channel, body).await,
            (Segment::Atomic(segment), false) => segment.publish::<(), _, _>(channel, body).await,
            (Segment::Atomic(segment), true) => segment.spublish::<(), _, _>(channel, body).await,
        }
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
    fn close_atomic(
        &self,
        segment: &Segment,
        ops: &mut Vec<Self::Op>,
    ) -> impl Future<Output = Result<(), Error>> + Send;

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
    /// The read's `COUNT`: this many settles fill the window.
    capacity: usize,
    /// Deliveries that arrived and were not yielded yet, on a form whose reads are not counted
    /// in `outstanding`.
    waiting: usize,
    committed: Vec<Segment>,
    ops: Vec<Op>,
    /// The ops of the delivery being settled, for an atomic segment to take.
    scratch: Vec<Op>,
    /// The ops of the members of a batch settled before its last one, by slot: the batch's segment
    /// takes them when its last member settles.
    staged: Vec<Vec<Op>>,
    /// A flush that failed, reported once on the delivery stream.
    failure: Option<RedisError>,
}

/// The window of one pipelined subscription.
pub(crate) struct Window<F: Form> {
    form: F,
    segments: Arc<Segments>,
    /// The subscription, named in a flush failure.
    name: Arc<str>,
    owed: Mutex<Owed<F::Op>>,
}

impl<F: Form> Debug for Window<F> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Window")
            .field("name", &self.name)
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
        rounds: Arc<Rounds>,
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
                rounds,
                atomic,
                hash_slot,
                slots: Mutex::new(Slots::default()),
            }),
            name,
            owed: Mutex::new(Owed {
                outstanding: 0,
                settled: 0,
                capacity: capacity.max(1),
                waiting: 0,
                committed: Vec::new(),
                ops: Vec::new(),
                scratch: Vec::new(),
                staged: Vec::new(),
                failure: None,
            }),
        }
    }

    fn owed(&self) -> MutexGuard<'_, Owed<F::Op>> {
        self.owed.lock().expect("pipeline window poisoned")
    }

    /// Sets how many settles fill the window: the batch size a batch mount reads with.
    pub(crate) fn fill_at(&self, capacity: usize) {
        self.owed().capacity = capacity.max(1);
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
        self.open_batch(1)
    }

    /// One slot for a batch of `members` deliveries: the batch's segment.
    pub(crate) fn open_batch(&self, members: u32) -> Round {
        Round {
            segments: Arc::clone(&self.segments),
            slot: self.segments.open(members),
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
        let closed = self.segments.close(round.slot, commit);
        if closed.is_some() {
            self.segments.rounds.leave(round);
        }
        let (mut result, closing) = {
            let mut guard = self.owed();
            let owed = &mut *guard;
            let slot = round.slot as usize;
            match closed {
                // A batch member before its last: its settle waits for the batch's.
                None => {
                    if owed.staged.len() <= slot {
                        owed.staged.resize_with(slot + 1, Vec::new);
                    }
                    (settle(&self.form, &mut owed.staged[slot]), None)
                }
                Some(Closed {
                    segment: Some(segment),
                    clean: true,
                }) if commit => {
                    let mut scratch = std::mem::take(&mut owed.scratch);
                    if let Some(staged) = owed.staged.get_mut(slot) {
                        scratch.append(staged);
                    }
                    let result = settle(&self.form, &mut scratch);
                    drop(guard);
                    (result, Some((segment, scratch)))
                }
                // An outcome other than `ack` on any member drops the segment: what the handler
                // queued never leaves.
                Some(_) => {
                    if let Some(staged) = owed.staged.get_mut(slot) {
                        owed.ops.append(staged);
                    }
                    (settle(&self.form, &mut owed.ops), None)
                }
            }
        };
        let mut kept = None;
        if let Some((segment, mut scratch)) = closing {
            // A form that settles nothing answers `Unsupported` to an `ack`, and its segment is
            // committed all the same: what the handler queued still follows the outcome.
            let mut keep = matches!(result, Ok(()) | Err(AckError::Unsupported));
            if keep
                && self.segments.atomic
                && let Err(err) = self.close_atomic(&segment, &mut scratch).await
            {
                keep = false;
                result = Err(AckError::Broker(Box::new(err)));
            }
            kept = Some((keep.then_some(segment), scratch));
        }
        let flush = {
            let mut guard = self.owed();
            let owed = &mut *guard;
            if let Some((segment, mut scratch)) = kept {
                if let Some(segment) = segment {
                    owed.committed.push(segment);
                }
                owed.ops.append(&mut scratch);
                owed.scratch = scratch;
            }
            owed.outstanding = owed.outstanding.saturating_sub(1);
            owed.settled += 1;
            let due = (owed.outstanding == 0 && owed.waiting == 0) || owed.settled >= owed.capacity;
            let flush = due.then(|| Self::take(owed));
            drop(guard);
            flush
        };
        if let Some(flush) = flush {
            self.send(flush).await;
        }
        result
    }

    /// Gives a delivery's slot back without settling it: the delivery was dropped unsettled, so
    /// its segment is dropped and the entry stays with the broker.
    pub(crate) fn abandon(&self, round: &Round) {
        let closed = self.segments.close(round.slot, false);
        if closed.is_some() {
            self.segments.rounds.leave(round);
        }
        let mut guard = self.owed();
        let owed = &mut *guard;
        // The last member of a batch gives back what the members before it owe.
        if closed.is_some()
            && let Some(staged) = owed.staged.get_mut(round.slot as usize)
        {
            owed.ops.append(staged);
        }
        owed.outstanding = owed.outstanding.saturating_sub(1);
        drop(guard);
    }

    /// Takes what the window owes, for a flush on stop.
    pub(crate) fn drain(&self) -> impl Future<Output = ()> + Send + '_ {
        let flush = Self::take(&mut self.owed());
        async move { self.send(flush).await }
    }

    async fn close_atomic(&self, segment: &Segment, ops: &mut Vec<F::Op>) -> Result<(), Error> {
        self.form.close_atomic(segment, ops).await?;
        segment
            .pipeline()
            .custom::<(), Value>(
                transaction_edge("EXEC", self.segments.hash_slot),
                Vec::new(),
            )
            .await
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
