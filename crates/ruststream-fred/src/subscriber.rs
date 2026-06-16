//! Redis Streams subscriber driving `XREADGROUP` (fresh tail) or `XAUTOCLAIM` (reclaim).

use std::collections::{HashMap, VecDeque};
use std::fmt::{Debug, Formatter};
use std::time::Duration;

use fred::clients::Pool;
use fred::interfaces::StreamsInterface;
use fred::types::streams::XReadValue;
use futures::Stream;
use futures::stream::unfold;
use ruststream::{BatchSubscriber, Subscriber};

use crate::convert::{HEADER_PREFIX, parts_from_fields};
use crate::deadletter::{
    self, DELIVERY_COUNT_HEADER, IDLE_MS_HEADER, PoisonPolicy, REASON_MAX_DELIVERIES,
};
use crate::{error::RedisError, message::RedisMessage, stream::ReadMode};

/// One decoded stream entry: its ID and field map.
type Entry = (String, HashMap<String, Vec<u8>>);

/// `XREADGROUP` reply shape parsed as nested arrays rather than maps: the RESP2 reply is an array of
/// `[key, [[id, [field, value, ...]], ...]]`, which does not convert to fred's map-based
/// `XReadResponse` (the outer array is not a flat key/value list). Pairing into tuples does work, so
/// we collect the entry fields into a map ourselves.
type RawStreams = Vec<(String, Vec<(String, Vec<(String, Vec<u8>)>)>)>;

/// Cursor a fresh reclaim scan starts from (the whole pending list).
const RECLAIM_START: &str = "0-0";

fn duration_to_millis(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

/// A Redis Streams subscription bound to a consumer group.
///
/// Constructed by [`crate::RedisBroker::subscribe`] from a [`crate::RedisStream`] descriptor. The
/// read mode (fresh tail vs reclaim) is fixed at construction.
pub struct RedisSubscriber {
    pool: Pool,
    key: String,
    group: String,
    consumer: String,
    count: u64,
    block: Duration,
    mode: ReadMode,
    policy: PoisonPolicy,
    /// Reclaim cursor; advances across `XAUTOCLAIM` calls, unused in fresh mode.
    cursor: String,
    /// Entries fetched but not yet yielded.
    buffer: VecDeque<Entry>,
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
        count: u64,
        block: Duration,
        mode: ReadMode,
        policy: PoisonPolicy,
    ) -> Self {
        Self {
            pool,
            key,
            group,
            consumer,
            count,
            block,
            mode,
            policy,
            cursor: RECLAIM_START.to_owned(),
            buffer: VecDeque::new(),
        }
    }

    fn message(&self, id: String, fields: HashMap<String, Vec<u8>>) -> RedisMessage {
        let (payload, headers) = parts_from_fields(fields);
        RedisMessage::new(
            self.pool.clone(),
            self.key.clone(),
            self.group.clone(),
            id,
            payload,
            headers,
            self.policy.clone(),
        )
    }

    /// Fetches the next batch of entries into the buffer. A read that timed out with nothing
    /// pending leaves the buffer empty (the caller loops and reads again).
    async fn fetch(&mut self) -> Result<(), RedisError> {
        let entries = match self.mode.clone() {
            ReadMode::Fresh => self.fetch_fresh().await?,
            ReadMode::Reclaim { min_idle } => self.fetch_reclaim(min_idle).await?,
        };
        self.buffer.extend(entries);
        Ok(())
    }

    async fn fetch_fresh(&self) -> Result<Vec<Entry>, RedisError> {
        let resp: RawStreams = self
            .pool
            .xreadgroup(
                self.group.as_str(),
                self.consumer.as_str(),
                Some(self.count),
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
            .map(|(id, fields)| (id, fields.into_iter().collect()))
            .collect())
    }

    async fn fetch_reclaim(&mut self, min_idle: Duration) -> Result<Vec<Entry>, RedisError> {
        let (cursor, entries): (String, Vec<XReadValue<String, String, Vec<u8>>>) = self
            .pool
            .xautoclaim_values(
                self.key.as_str(),
                self.group.as_str(),
                self.consumer.as_str(),
                duration_to_millis(min_idle),
                self.cursor.as_str(),
                Some(self.count),
                false,
            )
            .await
            .map_err(RedisError::stream)?;
        self.cursor = cursor;
        // Nothing left to reclaim this pass: avoid a hot loop until more entries go stale.
        if entries.is_empty() {
            tokio::time::sleep(self.block).await;
            return Ok(entries);
        }
        // Plain reclaim with no poison policy: skip the extra XPENDING and deliver as-is.
        if !self.policy.is_active() {
            return Ok(entries);
        }
        self.enrich_reclaimed(entries).await
    }

    /// Annotates reclaimed entries with their native delivery count and idle time, and dead-letters
    /// (or drops) any that have exceeded `max_deliveries` instead of redelivering them.
    async fn enrich_reclaimed(&self, entries: Vec<Entry>) -> Result<Vec<Entry>, RedisError> {
        let meta = self.pending_meta().await?;
        let mut out = Vec::with_capacity(entries.len());
        for (id, mut fields) in entries {
            let (idle, count) = meta.get(&id).copied().unwrap_or((0, 0));
            if self.policy.is_poison(count) {
                self.dead_letter_reclaimed(&id, &fields).await?;
                continue;
            }
            insert_meta_header(&mut fields, DELIVERY_COUNT_HEADER, count);
            insert_meta_header(&mut fields, IDLE_MS_HEADER, idle);
            out.push((id, fields));
        }
        Ok(out)
    }

    /// Maps each of this consumer's pending entry IDs to its `(idle_ms, delivery_count)` via
    /// extended `XPENDING`, which - unlike `XAUTOCLAIM` - reports the native delivery count.
    async fn pending_meta(&self) -> Result<HashMap<String, (u64, u64)>, RedisError> {
        let rows: Vec<(String, String, u64, u64)> = self
            .pool
            .xpending(
                self.key.as_str(),
                self.group.as_str(),
                (0_u64, "-", "+", self.count, self.consumer.as_str()),
            )
            .await
            .map_err(RedisError::stream)?;
        Ok(rows
            .into_iter()
            .map(|(id, _consumer, idle, count)| (id, (idle, count)))
            .collect())
    }

    /// Routes a poison reclaimed entry to its dead-letter stream (or discards it when none is set),
    /// then `XACK`s it so it leaves the pending list.
    async fn dead_letter_reclaimed(
        &self,
        id: &str,
        fields: &HashMap<String, Vec<u8>>,
    ) -> Result<(), RedisError> {
        let (payload, headers) = parts_from_fields(fields.clone());
        deadletter::settle_poison_stream(
            &self.pool,
            &self.policy,
            &payload,
            &headers,
            REASON_MAX_DELIVERIES,
        )
        .await
        .map_err(RedisError::stream)?;
        let _: i64 = self
            .pool
            .xack(self.key.as_str(), self.group.as_str(), id)
            .await
            .map_err(RedisError::stream)?;
        Ok(())
    }
}

/// Injects a `u64`-valued well-known header into an entry's raw field map (under the `h:` prefix),
/// so it surfaces as a [`Headers`](ruststream::Headers) entry on the delivered message.
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
    /// the group's pending list and are redelivered (fresh mode) or reclaimable (reclaim mode).
    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        unfold(self, |s| async move {
            loop {
                if let Some((id, fields)) = s.buffer.pop_front() {
                    return Some((Ok(s.message(id, fields)), s));
                }
                // An empty fetch (a blocking read that timed out) just loops and reads again.
                if let Err(err) = s.fetch().await {
                    return Some((Err(err), s));
                }
            }
        })
    }
}

impl BatchSubscriber for RedisSubscriber {
    type Batch = Vec<RedisMessage>;

    /// Yields one batch per non-empty read (`XREADGROUP COUNT` / `XAUTOCLAIM`), up to
    /// [`RedisStream::count`](crate::RedisStream::count) entries. Never yields an empty batch.
    ///
    /// # Cancel safety
    ///
    /// Same as [`Subscriber::stream`]: dropping the stream mid-read leaves fetched-but-unacked
    /// entries in the pending list.
    fn batches(&mut self) -> impl Stream<Item = Result<Self::Batch, Self::Error>> + Send + '_ {
        unfold(self, |s| async move {
            loop {
                if !s.buffer.is_empty() {
                    // Move the buffer out first so `s.message` can borrow `s` without overlapping
                    // a live mutable borrow of `s.buffer`.
                    let entries = std::mem::take(&mut s.buffer);
                    let batch = entries
                        .into_iter()
                        .map(|(id, fields)| s.message(id, fields))
                        .collect::<Vec<_>>();
                    return Some((Ok(batch), s));
                }
                if let Err(err) = s.fetch().await {
                    return Some((Err(err), s));
                }
            }
        })
    }
}
