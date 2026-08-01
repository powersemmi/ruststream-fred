//! Repositioning a consumer group over the stream: [`EntryId`], [`RedisGroupPosition`], and the
//! [`RedisGroupSeeker`] handle behind the core `Seekable` capability.
//!
//! Redis Streams keep every entry until the stream is trimmed, so a consumer group can be moved
//! back over history or forward past a region. The move is `XGROUP SETID`, which rewrites the
//! group's cursor - and a group has one cursor, so the move applies to **every consumer of that
//! group**, not just the subscription that asked. The type names say `Group` for exactly that
//! reason; see [`RedisGroupSeeker`] for the full contract.

use std::fmt::{Display, Formatter};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use fred::clients::Pool;
use fred::interfaces::StreamsInterface;
use ruststream::Seeker;

use crate::error::RedisError;

/// A Redis Streams entry id: the `<milliseconds>-<sequence>` pair the server assigns to every
/// entry.
///
/// Parsed on construction, so a value of this type is always a well-formed id and comparisons
/// order entries the way the stream does. The `<milliseconds>` half alone is accepted too
/// (`"1700000000000"` means `1700000000000-0`), matching what Redis accepts on the wire.
///
/// # Examples
///
/// ```
/// use ruststream_fred::EntryId;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let id: EntryId = "1700000000000-4".parse()?;
/// assert_eq!(id.milliseconds(), 1_700_000_000_000);
/// assert_eq!(id.sequence(), 4);
/// assert_eq!(id.to_string(), "1700000000000-4");
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EntryId {
    milliseconds: u64,
    sequence: u64,
}

impl EntryId {
    /// The lowest id the stream can express, below every real entry (Redis rejects `0-0` as an
    /// entry id, so nothing can sit at it).
    pub const ZERO: Self = Self::new(0, 0);

    /// Builds an id from its two halves.
    #[must_use]
    pub const fn new(milliseconds: u64, sequence: u64) -> Self {
        Self {
            milliseconds,
            sequence,
        }
    }

    /// The `<milliseconds>` half: the entry's server-side timestamp.
    #[must_use]
    pub const fn milliseconds(&self) -> u64 {
        self.milliseconds
    }

    /// The `<sequence>` half: the counter distinguishing entries added within one millisecond.
    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    /// The id immediately below this one, saturating at [`ZERO`](Self::ZERO).
    ///
    /// Sequence numbers are dense within a millisecond, so the predecessor of `<ms>-0` is
    /// `<ms - 1>-<u64::MAX>`. Used to turn "resume *at* this entry" into the exclusive cursor
    /// `XGROUP SETID` takes.
    #[must_use]
    pub const fn previous(&self) -> Self {
        match (self.milliseconds, self.sequence) {
            (0, 0) => Self::ZERO,
            (ms, 0) => Self::new(ms - 1, u64::MAX),
            (ms, seq) => Self::new(ms, seq - 1),
        }
    }
}

impl Display for EntryId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.milliseconds, self.sequence)
    }
}

impl FromStr for EntryId {
    type Err = RedisError;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        let invalid = || {
            RedisError::InvalidOptions(format!(
                "`{raw}` is not a redis stream entry id (expected `<milliseconds>-<sequence>`)"
            ))
        };
        let (ms, seq) = match raw.split_once('-') {
            Some((ms, seq)) => (ms, seq),
            // Redis accepts a bare `<ms>`, which means `<ms>-0`.
            None => (raw, "0"),
        };
        Ok(Self::new(
            ms.parse().map_err(|_| invalid())?,
            seq.parse().map_err(|_| invalid())?,
        ))
    }
}

/// Where a consumer group's cursor sits, for [`RedisGroupSeeker::seek`] and the
/// `start_at(..)` clause of `#[subscriber]`.
///
/// The cursor is **group-wide**: it belongs to the consumer group, not to one subscription, so
/// moving it moves it for every consumer reading that group.
///
/// # Examples
///
/// ```
/// use ruststream_fred::{EntryId, RedisGroupPosition};
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let replay_all = RedisGroupPosition::beginning();
/// let skip_backlog = RedisGroupPosition::end();
/// let resume = RedisGroupPosition::after("1700000000000-4".parse::<EntryId>()?);
/// # let _ = (replay_all, skip_backlog, resume);
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RedisGroupPosition {
    /// The oldest entry the stream still retains: the group replays everything it holds.
    Beginning,
    /// The tail: only entries added after the seek are delivered.
    End,
    /// The first entry *after* `id`. The cursor is exclusive, exactly like the id argument of
    /// `XGROUP SETID`, so the entry at `id` itself is not delivered again.
    After(EntryId),
}

impl RedisGroupPosition {
    /// The oldest retained entry. Constructor form of [`Beginning`](Self::Beginning).
    #[must_use]
    pub const fn beginning() -> Self {
        Self::Beginning
    }

    /// Only entries added from now on. Constructor form of [`End`](Self::End).
    #[must_use]
    pub const fn end() -> Self {
        Self::End
    }

    /// Resume with the entry following `id`. Constructor form of [`After`](Self::After).
    #[must_use]
    pub const fn after(id: EntryId) -> Self {
        Self::After(id)
    }

    /// The id `XGROUP SETID` takes for this position.
    fn as_xid(self) -> String {
        match self {
            Self::Beginning => EntryId::ZERO.to_string(),
            Self::End => "$".to_owned(),
            Self::After(id) => id.to_string(),
        }
    }
}

/// Moves a consumer group's cursor.
///
/// The handle behind the core `Seekable` capability, minted with
/// [`Seekable::seeker`](ruststream::Seekable::seeker) or injected into a handler as a
/// `Seek(seeker): Seek<RedisGroupSeeker>` parameter.
///
/// # A seek is group-wide
///
/// Redis keeps one cursor per consumer group, so `XGROUP SETID` moves the read position for
/// **every consumer of that group**, not only for the subscription this seeker came from. That is
/// unlike a partitioned log, where a seek is scoped to one consumer instance. Treat a seek as an
/// operation on the group: replaying a range replays it for the whole worker pool, and skipping
/// forward skips for all of them.
///
/// # What a seek does not do
///
/// * It does not clear the pending entries list. Entries already delivered and not acknowledged
///   stay pending and remain reachable through the reclaim path
///   ([`RedisStream::reclaim`](crate::RedisStream::reclaim)), whichever way the cursor moved.
/// * It does not cancel delayed retries. Copies already scheduled in a
///   [`DelayedRetry::DurableZset`](crate::DelayedRetry::DurableZset) queue are keyed by their due
///   time, not by the cursor, so they are appended to the stream when they fall due regardless of
///   where the group is reading.
/// * It does not reset delivery counts. A replayed entry is delivered again, so its **native**
///   delivery count (the one the reclaim path reads and
///   [`RedisStream::max_deliveries`](crate::RedisStream::max_deliveries) caps) grows with each
///   replay; the framework retry-count header only moves on an actual `nack`.
///
/// # Timing
///
/// The cursor changes as soon as `seek` returns, but a subscription parked in a blocking
/// `XREADGROUP` observes it only on its next read - within one
/// [`RedisStream::block`](crate::RedisStream::block) interval. Entries selected under the old
/// cursor are discarded rather than delivered, so a seek never yields a message from the position
/// it moved away from.
///
/// # Examples
///
/// ```no_run
/// use ruststream::{Broker, Seekable, Seeker};
/// use ruststream_fred::{RedisBroker, RedisGroupPosition, RedisStream};
///
/// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
/// let connected = RedisBroker::standalone("redis://localhost:6379").connect().await?;
/// let subscriber = connected
///     .subscribe(RedisStream::new("orders").group("workers"))
///     .await?;
///
/// // Minted before the stream opens; usable while it runs.
/// let seeker = subscriber.seeker();
/// seeker.seek(RedisGroupPosition::beginning()).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct RedisGroupSeeker {
    pool: Pool,
    key: String,
    group: String,
    /// Shared with the subscription: bumped on every seek so entries selected under the old
    /// cursor are recognised as stale and dropped instead of delivered.
    generation: Arc<AtomicU64>,
}

impl std::fmt::Debug for RedisGroupSeeker {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisGroupSeeker")
            .field("key", &self.key)
            .field("group", &self.group)
            .finish_non_exhaustive()
    }
}

impl RedisGroupSeeker {
    pub(crate) fn new(pool: Pool, key: String, group: String, generation: Arc<AtomicU64>) -> Self {
        Self {
            pool,
            key,
            group,
            generation,
        }
    }
}

impl Seeker for RedisGroupSeeker {
    type Position = RedisGroupPosition;
    type Error = RedisError;

    /// Moves the group cursor with `XGROUP SETID`.
    ///
    /// # Errors
    ///
    /// Returns [`RedisError::Stream`] when the group or the stream does not exist, or the
    /// command fails.
    async fn seek(&self, to: RedisGroupPosition) -> Result<(), RedisError> {
        let _: String = self
            .pool
            .xgroup_setid(self.key.as_str(), self.group.as_str(), to.as_xid())
            .await
            .map_err(RedisError::stream)?;
        // Bumped after the cursor moved, so a reader that observes the new generation is
        // guaranteed to read under the new cursor.
        self.generation.fetch_add(1, Ordering::Release);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entry_ids_parse_and_render() {
        let id: EntryId = "1700000000000-4".parse().expect("valid id");
        assert_eq!(id, EntryId::new(1_700_000_000_000, 4));
        assert_eq!(id.to_string(), "1700000000000-4");
        // A bare millisecond half means sequence zero, as on the wire.
        assert_eq!(
            "5".parse::<EntryId>().expect("valid id"),
            EntryId::new(5, 0)
        );
    }

    #[test]
    fn malformed_entry_ids_are_rejected() {
        for raw in ["", "-", "abc", "5-x", "5-6-7"] {
            assert!(
                raw.parse::<EntryId>().is_err(),
                "`{raw}` must not parse as an entry id"
            );
        }
    }

    // The predecessor is what turns a captured delivery into the exclusive cursor that redelivers
    // it, so the sequence and millisecond borrows must both be right.
    #[test]
    fn previous_walks_back_one_id() {
        assert_eq!(EntryId::new(5, 3).previous(), EntryId::new(5, 2));
        assert_eq!(EntryId::new(5, 0).previous(), EntryId::new(4, u64::MAX));
        assert_eq!(EntryId::ZERO.previous(), EntryId::ZERO);
        assert_eq!(EntryId::new(0, 1).previous(), EntryId::ZERO);
    }

    #[test]
    fn positions_map_to_setid_arguments() {
        assert_eq!(RedisGroupPosition::beginning().as_xid(), "0-0");
        assert_eq!(RedisGroupPosition::end().as_xid(), "$");
        assert_eq!(
            RedisGroupPosition::after(EntryId::new(7, 2)).as_xid(),
            "7-2"
        );
    }
}
