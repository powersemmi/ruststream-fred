//! Which delivery's round a publish joins.
//!
//! A reply and a slot publish reach this crate's publishers with no delivery in hand. What they do
//! have is the task: the runtime runs one delivery's handler, its reply and its settle in one task.
//! A delivery of a pipelined subscription records its round against the task it is handled in,
//! `pipeline.bind(&out)` records a slot's publisher against it, and a publisher asks here, before
//! it publishes, whether the publish belongs to a round.

use std::fmt::{Debug, Formatter};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use tokio::task::{self, Id};

use super::window::Round;

/// One delivery in flight on a pipelined subscription, and the publishers bound to its round.
struct Entry {
    task: Id,
    round: Round,
    bound: Vec<u64>,
}

/// The rounds of the deliveries in flight on one connection's pipelined subscriptions.
#[derive(Default)]
pub(crate) struct Rounds {
    /// How many entries there are: a connection with no delivery in flight on a pipelined
    /// subscription answers every publish on one relaxed load.
    open: AtomicUsize,
    entries: Mutex<Vec<Entry>>,
    next_publisher: AtomicU64,
}

impl Debug for Rounds {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Rounds")
            .field("open", &self.open.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl Rounds {
    /// A name for one publisher and its clones, which is what a binding names.
    pub(crate) fn publisher(&self) -> u64 {
        self.next_publisher.fetch_add(1, Ordering::Relaxed)
    }

    fn with<R>(&self, apply: impl FnOnce(&mut Vec<Entry>) -> R) -> R {
        let mut entries = self.entries.lock().expect("round registry poisoned");
        let result = apply(&mut entries);
        self.open.store(entries.len(), Ordering::Relaxed);
        drop(entries);
        result
    }

    /// Records that `round` is handled in the current task.
    ///
    /// The runtime reads a delivery's headers in the task that handles it, before the handler,
    /// so the last task to read them is the one its reply is published from.
    pub(crate) fn enter(&self, round: &Round) {
        let Some(current) = task::try_id() else {
            return;
        };
        self.with(|entries| {
            if let Some(entry) = entries.iter_mut().find(|entry| entry.round.same(round)) {
                entry.task = current;
            } else {
                entries.push(Entry {
                    task: current,
                    round: round.clone(),
                    bound: Vec::new(),
                });
            }
        });
    }

    /// Records that `publisher` publishes into `round` from the current task.
    pub(crate) fn bind(&self, round: &Round, publisher: u64) {
        let Some(current) = task::try_id() else {
            return;
        };
        self.with(|entries| {
            if let Some(entry) = entries.iter_mut().find(|entry| entry.round.same(round)) {
                entry.task = current;
                if !entry.bound.contains(&publisher) {
                    entry.bound.push(publisher);
                }
            } else {
                entries.push(Entry {
                    task: current,
                    round: round.clone(),
                    bound: vec![publisher],
                });
            }
        });
    }

    /// Forgets `round` once its delivery has settled.
    pub(crate) fn leave(&self, round: &Round) {
        if self.open.load(Ordering::Relaxed) == 0 {
            return;
        }
        self.with(|entries| entries.retain(|entry| !entry.round.same(round)));
    }

    /// The round a publish by `publisher` from the current task joins: the one it is bound to,
    /// or, for a reply marked to join, the round of the delivery the task handles.
    pub(crate) fn joined(&self, publisher: u64, reply: bool) -> Option<Round> {
        if self.open.load(Ordering::Relaxed) == 0 {
            return None;
        }
        let current = task::try_id()?;
        let entries = self.entries.lock().expect("round registry poisoned");
        let round = entries
            .iter()
            .find(|entry| entry.task == current && (reply || entry.bound.contains(&publisher)))
            .map(|entry| entry.round.clone());
        drop(entries);
        round
    }
}
