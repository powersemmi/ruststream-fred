//! Redis Streams subscriber driving `XREADGROUP` (the fresh tail, or claim-and-read on Redis 8.4
//! and later) or `XAUTOCLAIM` (reclaim).

use std::collections::{HashMap, VecDeque};
use std::fmt::{Debug, Formatter};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use fred::clients::{Client, Pool};
use fred::interfaces::{ClientLike, StreamsInterface};
use fred::types::streams::XReadValue;
use fred::types::{CustomCommand, Value};
use futures::Stream;
use futures::stream::unfold;
use ruststream::{BatchSubscriber, Seekable, Subscriber};

use crate::claim::{self, ClaimedEntry};
use crate::convert::{HEADER_PREFIX, parts_from_fields};
use crate::delay::{self, DelayConfig};
use crate::message::{DELIVERY_COUNT_HEADER, IDLE_MS_HEADER, RedisMessage};
use crate::pipeline::{RoundMessage, StreamForm, Window};
use crate::seek::{EntryId, RedisGroupSeeker};
use crate::{error::RedisError, stream::ReadMode};

/// One decoded stream entry: its id, its field map, and the server's delivery count where the read
/// mode reports one.
///
/// The count reaches the delivery as a value rather than only as a header, because the runtime
/// reads it to apply the registration's `max_attempts` declaration.
struct Entry {
    id: String,
    fields: HashMap<String, Vec<u8>>,
    /// Deliveries of this entry counting this one, on the two modes that claim. `None` on the
    /// fresh tail, which claims nothing.
    delivered: Option<u64>,
}

/// `XREADGROUP` reply shape parsed as nested arrays rather than maps: the RESP2 reply is an array of
/// `[key, [[id, [field, value, ...]], ...]]`, which does not convert to fred's map-based
/// `XReadResponse` (the outer array is not a flat key/value list). Pairing into tuples does work, so
/// we collect the entry fields into a map ourselves.
type RawStreams = Vec<(String, Vec<(String, Vec<(String, Vec<u8>)>)>)>;

/// Cursor a fresh reclaim scan starts from (the whole pending list).
const RECLAIM_START: &str = "0-0";

/// The command the claiming mode sends. It goes out as a custom command because `CLAIM` changes
/// the reply shape, which the typed `xreadgroup` of `fred` cannot parse.
const XREADGROUP: &str = "XREADGROUP";

/// `COUNT` for a read on the single-message path, where the framework names no batch size: a read
/// that fetched one entry per round trip would spend a round trip per message, so the loop
/// prefetches and drains the buffer between reads. Batches take their own `COUNT` from the size the
/// mount site asked for instead.
pub(crate) const PREFETCH: u64 = 64;

fn duration_to_millis(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

/// A Redis Streams subscription bound to a consumer group.
///
/// Constructed by [`crate::ConnectedRedisBroker::subscribe`] from a [`crate::RedisStream`]
/// descriptor. The read mode (fresh tail, reclaim, or claim-and-read) is fixed at construction.
pub struct RedisSubscriber {
    pool: Pool,
    key: String,
    group: String,
    consumer: String,
    block: Duration,
    mode: ReadMode,
    /// Set when the subscription opted into a durable ZSET delay queue; drives native `nack_after`
    /// on each delivery and the due-entry sweep on each fetch.
    delay: Option<DelayConfig>,
    /// Reclaim cursor; advances across `XAUTOCLAIM` calls, unused in fresh mode.
    cursor: String,
    /// Entries fetched but not yet yielded.
    buffer: VecDeque<Entry>,
    /// Bumped by every [`RedisGroupSeeker`] minted off this subscription. Shared, because a seek
    /// can land while this subscriber is parked in a blocking read.
    generation: Arc<AtomicU64>,
    /// The generation the buffered entries were selected under. A mismatch means a seek moved the
    /// cursor after they were chosen, so they belong to the position the group left.
    buffer_generation: u64,
    /// Minted once, when the subscription opens, and handed to every delivery so its context can
    /// carry the handle without building one per message.
    seeker: Arc<RedisGroupSeeker>,
}

impl Debug for RedisSubscriber {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisSubscriber")
            .field("key", &self.key)
            .field("group", &self.group)
            .field("consumer", &self.consumer)
            .field("mode", &self.mode)
            .finish_non_exhaustive()
    }
}

impl RedisSubscriber {
    #[allow(
        clippy::too_many_arguments,
        reason = "internal constructor mirroring the descriptor"
    )]
    pub(crate) fn new(
        pool: Pool,
        key: String,
        group: String,
        consumer: String,
        block: Duration,
        mode: ReadMode,
        delay: Option<DelayConfig>,
    ) -> Self {
        let generation = Arc::new(AtomicU64::new(0));
        let seeker = Arc::new(RedisGroupSeeker::new(
            pool.clone(),
            key.as_str(),
            group.as_str(),
            Arc::clone(&generation),
        ));
        Self {
            pool,
            key,
            group,
            consumer,
            block,
            mode,
            delay,
            cursor: RECLAIM_START.to_owned(),
            buffer: VecDeque::new(),
            generation,
            buffer_generation: 0,
            seeker,
        }
    }

    /// Builds the delivery for one fetched entry.
    ///
    /// # Errors
    ///
    /// Returns [`RedisError::Stream`] when the server sent an id that is not a well-formed
    /// `<milliseconds>-<sequence>` pair, which would leave the delivery unable to report its
    /// position.
    fn message(&self, entry: Entry) -> Result<RedisMessage, RedisError> {
        let id = entry.id;
        let parsed: EntryId = id.parse()?;
        let (payload, headers) = parts_from_fields(entry.fields);
        Ok(RedisMessage::new(
            self.pool.clone(),
            self.key.clone(),
            self.group.clone(),
            id,
            parsed,
            payload,
            headers,
            self.delay.clone(),
            entry.delivered,
            Arc::clone(&self.seeker),
            self.mode.requeue(),
        ))
    }

    /// Drops entries selected before a seek moved the group cursor: they belong to the position
    /// the group left, so delivering them would contradict the reposition. Returns how many it
    /// dropped.
    fn discard_stale(&mut self) -> usize {
        let current = self.generation.load(Ordering::Acquire);
        if self.buffer_generation == current {
            return 0;
        }
        let dropped = self.buffer.len();
        self.buffer.clear();
        self.buffer_generation = current;
        dropped
    }

    /// The stream key this subscription reads.
    pub(crate) fn key(&self) -> &str {
        &self.key
    }

    /// The settle side of this subscription inside a window.
    pub(crate) fn round_form(&self) -> StreamForm {
        StreamForm::new(
            self.key.as_str(),
            self.group.as_str(),
            self.delay.clone(),
            self.mode.requeue(),
        )
    }

    /// The window's client: a connection of the pool the flushes go out on.
    pub(crate) fn round_client(&self) -> Client {
        self.pool.next().clone()
    }

    /// Yields one delivery per entry, each with a slot in `window`, and reports a failed flush of
    /// the window once.
    pub(crate) fn round_stream<'a>(
        &'a mut self,
        window: &'a Arc<Window<StreamForm>>,
    ) -> impl Stream<Item = Result<RoundMessage<StreamForm>, RedisError>> + Send + 'a {
        unfold(self, move |s| async move {
            loop {
                if let Some(failure) = window.take_failure() {
                    return Some((Err(failure), s));
                }
                window.discarded(s.discard_stale());
                if let Some(entry) = s.buffer.pop_front() {
                    let delivery = match s.message(entry) {
                        Ok(delivery) => delivery,
                        Err(err) => {
                            window.discarded(1);
                            return Some((Err(err), s));
                        }
                    };
                    let round = window.open();
                    return Some((
                        Ok(RoundMessage::new(delivery, Arc::clone(window), round)),
                        s,
                    ));
                }
                if let Err(err) = s.fetch(PREFETCH).await {
                    return Some((Err(err), s));
                }
                window.fetched(s.buffer.len());
            }
        })
    }

    /// Fetches up to `count` entries into the buffer. A read that timed out with nothing pending
    /// leaves the buffer empty (the caller loops and reads again).
    async fn fetch(&mut self, count: u64) -> Result<(), RedisError> {
        // Replay any due delayed-retry entries before reading, so they re-enter the stream and get
        // delivered through the normal read path. Granularity is the read block interval.
        if let Some(cfg) = &self.delay {
            delay::sweep_due(&self.pool, cfg, &self.key).await?;
        }
        // Captured before the read, not after: a blocking read selects its entries against the
        // cursor as it stood when the read started, so a seek that lands mid-read invalidates
        // whatever comes back.
        let selected_at = self.generation.load(Ordering::Acquire);
        let entries = match self.mode.clone() {
            ReadMode::Fresh => self.fetch_fresh(count).await?,
            ReadMode::Reclaim { min_idle } => self.fetch_reclaim(min_idle, count).await?,
            ReadMode::Claiming { min_idle } => self.fetch_claiming(min_idle, count).await?,
        };
        if selected_at != self.generation.load(Ordering::Acquire) {
            // A seek overtook this read; drop its entries and let the caller read again.
            return Ok(());
        }
        self.buffer_generation = selected_at;
        self.buffer.extend(entries);
        Ok(())
    }

    async fn fetch_fresh(&self, count: u64) -> Result<Vec<Entry>, RedisError> {
        let resp: RawStreams = self
            .pool
            .xreadgroup(
                self.group.as_str(),
                self.consumer.as_str(),
                Some(count),
                Some(duration_to_millis(self.block)),
                false,
                self.key.as_str(),
                ">",
            )
            .await
            .map_err(RedisError::stream)?;
        let entries = resp
            .into_iter()
            .find(|(key, _)| key == &self.key)
            .map(|(_, entries)| entries)
            .unwrap_or_default();
        Ok(entries
            .into_iter()
            .map(|(id, fields)| Entry {
                id,
                fields: fields.into_iter().collect(),
                delivered: None,
            })
            .collect())
    }

    /// One `XREADGROUP GROUP g c COUNT n BLOCK ms CLAIM min-idle STREAMS key >`: the group's
    /// entries idle at least `min_idle` first, longest idle first, then fresh ones up to the
    /// remaining `COUNT`. The read blocks on both a new entry and an entry ageing past the
    /// threshold.
    ///
    /// It goes out as a custom command because `CLAIM` makes every entry `[id, fields, idle-ms,
    /// delivery-count]`, which the typed `xreadgroup` of `fred` parses as a two-element entry and
    /// rejects. The two extra fields become the headers the reclaim path already writes, so a
    /// handler reads them the same way on either mode.
    async fn fetch_claiming(
        &self,
        min_idle: Duration,
        count: u64,
    ) -> Result<Vec<Entry>, RedisError> {
        // The key is hashed into the command's cluster slot: it is not the first argument, so the
        // default "hash the first key" policy would route the read by the group name.
        let command = CustomCommand::new_static(XREADGROUP, self.key.as_str(), true);
        // Every argument goes as a string, which is what a Redis command is on the wire.
        let args: Vec<Value> = [
            "GROUP",
            self.group.as_str(),
            self.consumer.as_str(),
            "COUNT",
            &count.to_string(),
            "BLOCK",
            &duration_to_millis(self.block).to_string(),
            "CLAIM",
            &duration_to_millis(min_idle).to_string(),
            "STREAMS",
            self.key.as_str(),
            ">",
        ]
        .into_iter()
        .map(Value::from)
        .collect();

        let frame = self
            .pool
            .custom_raw(command, args)
            .await
            .map_err(RedisError::stream)?;
        let claimed = claim::decode_reply(&frame, &self.key)?;
        Ok(Self::annotate_claimed(claimed))
    }

    /// Hands each claimed entry its two counters: as headers a handler can read, and as the
    /// delivery count the runtime caps on.
    ///
    /// Unlike the reclaim path this needs no round trip of its own: `CLAIM` reports both counters
    /// with the entry. It reports them as of the delivery before this one, so the count this
    /// delivery makes is added on, which is where the reclaim path's `XPENDING` value already
    /// stands - the two modes then cap at the same attempt.
    fn annotate_claimed(claimed: Vec<ClaimedEntry>) -> Vec<Entry> {
        claimed
            .into_iter()
            .map(|entry| {
                let mut fields = entry.fields;
                insert_meta_header(&mut fields, DELIVERY_COUNT_HEADER, entry.delivery_count);
                insert_meta_header(&mut fields, IDLE_MS_HEADER, entry.idle_ms);
                Entry {
                    id: entry.id,
                    fields,
                    delivered: Some(entry.delivery_count + 1),
                }
            })
            .collect()
    }

    async fn fetch_reclaim(
        &mut self,
        min_idle: Duration,
        count: u64,
    ) -> Result<Vec<Entry>, RedisError> {
        let (cursor, entries): (String, Vec<XReadValue<String, String, Vec<u8>>>) = self
            .pool
            .xautoclaim_values(
                self.key.as_str(),
                self.group.as_str(),
                self.consumer.as_str(),
                duration_to_millis(min_idle),
                self.cursor.as_str(),
                Some(count),
                false,
            )
            .await
            .map_err(RedisError::stream)?;
        self.cursor = cursor;
        // Nothing left to reclaim this pass: avoid a hot loop until more entries go stale.
        if entries.is_empty() {
            tokio::time::sleep(self.block).await;
            return Ok(Vec::new());
        }
        self.enrich_reclaimed(entries, count).await
    }

    /// Annotates reclaimed entries with their native delivery count and idle time.
    ///
    /// `XAUTOCLAIM` does not report either, so this costs one `XPENDING` per read that found
    /// something - not per entry. It is what the mode answers `redelivery_count` with, which is
    /// how a `max_attempts` declaration at the mount site reaches a reclaimed delivery.
    async fn enrich_reclaimed(
        &self,
        entries: Vec<XReadValue<String, String, Vec<u8>>>,
        limit: u64,
    ) -> Result<Vec<Entry>, RedisError> {
        let meta = self.pending_meta(limit).await?;
        let mut out = Vec::with_capacity(entries.len());
        for (id, mut fields) in entries {
            let (idle, count) = meta.get(&id).copied().unwrap_or((0, 0));
            insert_meta_header(&mut fields, DELIVERY_COUNT_HEADER, count);
            insert_meta_header(&mut fields, IDLE_MS_HEADER, idle);
            out.push(Entry {
                id,
                fields,
                delivered: Some(count),
            });
        }
        Ok(out)
    }

    /// Maps each of this consumer's pending entry IDs to its `(idle_ms, delivery_count)` via
    /// extended `XPENDING`, which - unlike `XAUTOCLAIM` - reports the native delivery count.
    async fn pending_meta(&self, limit: u64) -> Result<HashMap<String, (u64, u64)>, RedisError> {
        let rows: Vec<(String, String, u64, u64)> = self
            .pool
            .xpending(
                self.key.as_str(),
                self.group.as_str(),
                (0_u64, "-", "+", limit, self.consumer.as_str()),
            )
            .await
            .map_err(RedisError::stream)?;
        Ok(rows
            .into_iter()
            .map(|(id, _consumer, idle, count)| (id, (idle, count)))
            .collect())
    }
}

/// Injects a `u64`-valued well-known header into an entry's raw field map (under the `h:` prefix),
/// so it surfaces as a [`HeaderMap`](ruststream::HeaderMap) entry on the delivered message.
fn insert_meta_header(fields: &mut HashMap<String, Vec<u8>>, name: &str, value: u64) {
    fields.insert(
        format!("{HEADER_PREFIX}{name}"),
        value.to_string().into_bytes(),
    );
}

impl Subscriber for RedisSubscriber {
    type Message = RedisMessage;
    type Error = RedisError;

    /// Yields one message per entry, refilling from Redis when the local buffer drains.
    ///
    /// # Cancel safety
    ///
    /// Dropping the returned stream between items is safe. Dropping it while a read is in flight
    /// drops the read future; entries already delivered to this consumer but not yet acked stay in
    /// the group's pending list, where the reclaim and claiming modes pick them up again.
    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        unfold(self, |s| async move {
            loop {
                let _ = s.discard_stale();
                if let Some(entry) = s.buffer.pop_front() {
                    return Some((s.message(entry), s));
                }
                // An empty fetch (a blocking read that timed out) just loops and reads again.
                if let Err(err) = s.fetch(PREFETCH).await {
                    return Some((Err(err), s));
                }
            }
        })
    }
}

/// Repositioning the subscription moves its consumer group's cursor, which is shared by every
/// consumer of that group; see [`RedisGroupSeeker`] for the full contract.
impl Seekable for RedisSubscriber {
    type Seeker = RedisGroupSeeker;

    fn seeker(&self) -> RedisGroupSeeker {
        (*self.seeker).clone()
    }
}

impl BatchSubscriber for RedisSubscriber {
    type Batch = Vec<RedisMessage>;

    /// Yields one batch per non-empty read, native all the way down: `size` is the `COUNT` of the
    /// `XREADGROUP` (or `XAUTOCLAIM`) that fetches it, so the server never sends more than the
    /// batch holds. Never yields an empty batch.
    ///
    /// # Cancel safety
    ///
    /// Same as [`Subscriber::stream`]: dropping the stream mid-read leaves fetched-but-unacked
    /// entries in the pending list.
    fn batches(
        &mut self,
        size: NonZeroUsize,
    ) -> impl Stream<Item = Result<Self::Batch, Self::Error>> + Send + '_ {
        let count = u64::try_from(size.get()).unwrap_or(u64::MAX);
        unfold(self, move |s| async move {
            loop {
                let _ = s.discard_stale();
                if !s.buffer.is_empty() {
                    // Move the entries out first so `s.message` can borrow `s` without overlapping
                    // a live mutable borrow of `s.buffer`. The read already capped itself at
                    // `count`; the split is what keeps a batch within `size` when a re-entered
                    // subscription asks for a smaller one than the buffered read used.
                    let tail = s.buffer.split_off(size.get().min(s.buffer.len()));
                    let entries = std::mem::replace(&mut s.buffer, tail);
                    let batch = entries
                        .into_iter()
                        .map(|entry| s.message(entry))
                        .collect::<Result<Vec<_>, _>>();
                    return Some((batch, s));
                }
                if let Err(err) = s.fetch(count).await {
                    return Some((Err(err), s));
                }
            }
        })
    }
}
