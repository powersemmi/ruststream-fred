//! Streams and consumer groups on the in-process server.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Display, Formatter};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use fred::error::Error;
use fred::types::Value;
use tokio::time::Instant;

use super::args::{Args, bulk, int, ok, server_error, syntax};
use super::keyspace::{Keyspace, Write};
use super::{Server, State};

/// A stream entry id, `<milliseconds>-<sequence>`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct Id {
    ms: u64,
    seq: u64,
}

impl Id {
    const MAX: Self = Self {
        ms: u64::MAX,
        seq: u64::MAX,
    };

    fn text(self) -> Value {
        Value::String(self.to_string().as_str().into())
    }

    const fn next(self) -> Self {
        if self.seq == u64::MAX {
            Self {
                ms: self.ms.saturating_add(1),
                seq: 0,
            }
        } else {
            Self {
                ms: self.ms,
                seq: self.seq + 1,
            }
        }
    }
}

impl Display for Id {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.ms, self.seq)
    }
}

impl FromStr for Id {
    type Err = Error;

    fn from_str(text: &str) -> Result<Self, Error> {
        let invalid = || server_error("ERR Invalid stream ID specified as stream command argument");
        let (ms, seq) = match text.split_once('-') {
            Some((ms, seq)) => (
                ms.parse().map_err(|_| invalid())?,
                seq.parse().map_err(|_| invalid())?,
            ),
            None => (text.parse().map_err(|_| invalid())?, 0),
        };
        Ok(Self { ms, seq })
    }
}

type Fields = Arc<Vec<(Bytes, Bytes)>>;

/// One entry of a group's pending entries list.
#[derive(Debug, Clone)]
struct Pending {
    consumer: Bytes,
    /// When it was last delivered.
    at: Instant,
    /// How many times it has been delivered.
    count: u64,
}

#[derive(Debug, Default)]
struct Group {
    /// The id of the last entry delivered to the group.
    cursor: Id,
    pending: BTreeMap<Id, Pending>,
    consumers: BTreeSet<Bytes>,
}

/// A stream value.
#[derive(Debug, Default)]
pub(crate) struct Stream {
    entries: BTreeMap<Id, Fields>,
    last: Id,
    groups: BTreeMap<Bytes, Group>,
}

impl Stream {
    /// The names of its groups.
    pub(crate) fn groups(&self) -> Vec<Bytes> {
        self.groups.keys().cloned().collect()
    }
}

fn no_group(key: &[u8], group: &[u8], command: &str) -> Error {
    server_error(format!(
        "NOGROUP No such key '{}' or consumer group '{}' in {command} command",
        String::from_utf8_lossy(key),
        String::from_utf8_lossy(group),
    ))
}

fn entry_value(id: Id, fields: &Fields) -> Vec<Value> {
    vec![
        id.text(),
        Value::Array(
            fields
                .iter()
                .flat_map(|(field, value)| [bulk(field.clone()), bulk(value.clone())])
                .collect(),
        ),
    ]
}

fn millis(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

impl Keyspace {
    /// Whether `group` of `key` has entries past its cursor.
    pub(crate) fn has_new_entries(&self, key: &[u8], group: &[u8], now: Instant) -> bool {
        self.stream(key, now).ok().flatten().is_some_and(|stream| {
            stream
                .groups
                .get(group)
                .is_some_and(|group| stream.entries.range(group.cursor.next()..).next().is_some())
        })
    }

    /// The entries of `key` past the cursor of `group`.
    pub(crate) fn new_entries(&self, key: &[u8], group: &[u8], now: Instant) -> Vec<Id> {
        let Some(stream) = self.stream(key, now).ok().flatten() else {
            return Vec::new();
        };
        let Some(group) = stream.groups.get(group) else {
            return Vec::new();
        };
        stream
            .entries
            .range(group.cursor.next()..)
            .map(|(id, _)| *id)
            .collect()
    }
}

/// How `XADD` and `XTRIM` trim.
enum Trim {
    MaxLen(usize),
    MinId(Id),
}

impl Trim {
    fn parse(args: &mut Args<'_>) -> Result<Option<Self>, Error> {
        let trim = if args.word("MAXLEN") {
            let _ = args.word("=") || args.word("~");
            Some(Self::MaxLen(
                usize::try_from(args.int()?).map_err(|_| syntax())?,
            ))
        } else if args.word("MINID") {
            let _ = args.word("=") || args.word("~");
            Some(Self::MinId(args.text()?.parse()?))
        } else {
            None
        };
        if trim.is_some() && args.word("LIMIT") {
            args.int()?;
        }
        Ok(trim)
    }

    fn apply(&self, stream: &mut Stream) -> usize {
        let before = stream.entries.len();
        match self {
            Self::MaxLen(len) => {
                while stream.entries.len() > *len {
                    stream.entries.pop_first();
                }
            }
            Self::MinId(min) => stream.entries.retain(|id, _| id >= min),
        }
        before - stream.entries.len()
    }
}

impl State {
    pub(super) fn xadd(
        &mut self,
        server: &Server,
        args: &mut Args<'_>,
        now: Instant,
    ) -> Result<Value, Error> {
        let key = args.bytes()?;
        let no_create = args.word("NOMKSTREAM");
        let trim = Trim::parse(args)?;
        let requested = args.text()?;
        let values = args.rest()?;
        if values.is_empty() || values.len() % 2 != 0 {
            return Err(args.arity());
        }
        if no_create && !self.keys.exists(&key, now) {
            return Ok(Value::Null);
        }
        let now_ms = server.now_ms();
        let stream = self.keys.stream_mut(&key, now, true)?.expect("created");
        let id = match requested.as_str() {
            "*" => {
                if now_ms > stream.last.ms {
                    Id { ms: now_ms, seq: 0 }
                } else {
                    stream.last.next()
                }
            }
            explicit => match explicit.strip_suffix("-*") {
                Some(ms) => {
                    let ms: u64 = ms.parse().map_err(|_| syntax())?;
                    if ms == stream.last.ms {
                        stream.last.next()
                    } else {
                        Id { ms, seq: 0 }
                    }
                }
                None => explicit.parse()?,
            },
        };
        if id <= stream.last || id == Id::default() {
            return Err(server_error(
                "ERR The ID specified in XADD is equal or smaller than the target stream top item",
            ));
        }
        let fields: Vec<(Bytes, Bytes)> = values
            .chunks(2)
            .map(|pair| (pair[0].clone(), pair[1].clone()))
            .collect();
        stream.entries.insert(id, Arc::new(fields.clone()));
        stream.last = id;
        let trimmed = trim.map_or(0, |trim| trim.apply(stream));
        let groups = stream.groups();
        self.log(&key, Write::Fields(fields));
        for group in groups {
            if trimmed > 0 {
                self.recount_group(server, &key, &group);
            } else if self.group_readers(&key, &group).fresh
                && self
                    .stream_owed
                    .entry((key.clone(), group.clone()))
                    .or_default()
                    .insert(id)
            {
                server.owe(1);
            }
        }
        self.readded(server, &key);
        Ok(id.text())
    }

    pub(super) fn xlen(&self, args: &mut Args<'_>, now: Instant) -> Result<Value, Error> {
        let key = args.bytes()?;
        Ok(int(self
            .keys
            .stream(&key, now)?
            .map_or(0, |stream| stream.entries.len())))
    }

    pub(super) fn xrange(
        &self,
        args: &mut Args<'_>,
        now: Instant,
        reverse: bool,
    ) -> Result<Value, Error> {
        let key = args.bytes()?;
        let (first, second) = (args.text()?, args.text()?);
        let (start, end) = if reverse {
            (second, first)
        } else {
            (first, second)
        };
        let start = range_end(&start, Id::default(), true)?;
        let end = range_end(&end, Id::MAX, false)?;
        let count = if args.word("COUNT") {
            usize::try_from(args.int()?).unwrap_or(0)
        } else {
            usize::MAX
        };
        let Some(stream) = self.keys.stream(&key, now)? else {
            return Ok(Value::Array(Vec::new()));
        };
        if start > end {
            return Ok(Value::Array(Vec::new()));
        }
        let entries = stream.entries.range(start..=end);
        let pick = |(id, fields): (&Id, &Fields)| Value::Array(entry_value(*id, fields));
        Ok(Value::Array(if reverse {
            entries.rev().take(count).map(pick).collect()
        } else {
            entries.take(count).map(pick).collect()
        }))
    }

    pub(super) fn xdel(
        &mut self,
        server: &Server,
        args: &mut Args<'_>,
        now: Instant,
    ) -> Result<Value, Error> {
        let key = args.bytes()?;
        let ids = args
            .rest()?
            .iter()
            .map(|id| String::from_utf8_lossy(id).parse())
            .collect::<Result<Vec<Id>, Error>>()?;
        let Some(stream) = self.keys.stream_mut(&key, now, false)? else {
            return Ok(int(0));
        };
        let removed = ids
            .iter()
            .filter(|id| stream.entries.remove(id).is_some())
            .count();
        for group in stream.groups() {
            self.recount_group(server, &key, &group);
        }
        Ok(int(removed))
    }

    pub(super) fn xtrim(
        &mut self,
        server: &Server,
        args: &mut Args<'_>,
        now: Instant,
    ) -> Result<Value, Error> {
        let key = args.bytes()?;
        let trim = Trim::parse(args)?.ok_or_else(syntax)?;
        let Some(stream) = self.keys.stream_mut(&key, now, false)? else {
            return Ok(int(0));
        };
        let trimmed = trim.apply(stream);
        for group in stream.groups() {
            self.recount_group(server, &key, &group);
        }
        Ok(int(trimmed))
    }

    pub(super) fn xack(
        &mut self,
        server: &Server,
        args: &mut Args<'_>,
        now: Instant,
    ) -> Result<Value, Error> {
        let key = args.bytes()?;
        let group_name = args.bytes()?;
        let ids = args
            .rest()?
            .iter()
            .map(|id| String::from_utf8_lossy(id).parse())
            .collect::<Result<Vec<Id>, Error>>()?;
        if ids.is_empty() {
            return Err(args.arity());
        }
        let Some(group) = self
            .keys
            .stream_mut(&key, now, false)?
            .and_then(|stream| stream.groups.get_mut(&group_name))
        else {
            return Ok(int(0));
        };
        let acked: Vec<Id> = ids
            .into_iter()
            .filter(|id| group.pending.remove(id).is_some())
            .collect();
        let slot = (key, group_name);
        for id in &acked {
            if let Some(due) = self.claim_due.get_mut(&slot) {
                due.remove(id);
            }
            if self
                .stream_owed
                .get_mut(&slot)
                .is_some_and(|owed| owed.remove(id))
            {
                server.released(1);
            }
        }
        Ok(int(acked.len()))
    }

    pub(super) fn xgroup(
        &mut self,
        server: &Server,
        sub: &str,
        args: &mut Args<'_>,
        now: Instant,
    ) -> Result<Value, Error> {
        let key = args.bytes()?;
        let group_name = args.bytes()?;
        let reply = match sub {
            "CREATE" | "SETID" => {
                let position = args.text()?;
                let create = sub == "CREATE" && args.word("MKSTREAM");
                if args.word("ENTRIESREAD") {
                    args.int()?;
                }
                args.done()?;
                let Some(stream) = self.keys.stream_mut(&key, now, create)? else {
                    return Err(if sub == "CREATE" {
                        server_error(
                            "ERR The XGROUP subcommand requires the key to exist. Note that for \
                             CREATE you may want to use the MKSTREAM option to create an empty \
                             stream automatically.",
                        )
                    } else {
                        no_group(&key, &group_name, "XGROUP SETID")
                    });
                };
                let cursor = if position == "$" {
                    stream.last
                } else {
                    position.parse()?
                };
                if sub == "CREATE" {
                    if stream.groups.contains_key(&group_name) {
                        return Err(server_error("BUSYGROUP Consumer Group name already exists"));
                    }
                    stream.groups.insert(
                        group_name.clone(),
                        Group {
                            cursor,
                            ..Group::default()
                        },
                    );
                } else {
                    let group = stream
                        .groups
                        .get_mut(&group_name)
                        .ok_or_else(|| no_group(&key, &group_name, "XGROUP SETID"))?;
                    group.cursor = cursor;
                }
                ok()
            }
            "DESTROY" => {
                let removed = self
                    .keys
                    .stream_mut(&key, now, false)?
                    .and_then(|stream| stream.groups.remove(&group_name))
                    .is_some();
                self.claim_due.remove(&(key.clone(), group_name.clone()));
                int(usize::from(removed))
            }
            "CREATECONSUMER" | "DELCONSUMER" => {
                let consumer = args.bytes()?;
                let group = self
                    .keys
                    .stream_mut(&key, now, false)?
                    .and_then(|stream| stream.groups.get_mut(&group_name))
                    .ok_or_else(|| no_group(&key, &group_name, "XGROUP"))?;
                if sub == "CREATECONSUMER" {
                    int(usize::from(group.consumers.insert(consumer)))
                } else {
                    group.consumers.remove(&consumer);
                    let owned: Vec<Id> = group
                        .pending
                        .iter()
                        .filter(|(_, pending)| pending.consumer == consumer)
                        .map(|(id, _)| *id)
                        .collect();
                    for id in &owned {
                        group.pending.remove(id);
                    }
                    int(owned.len())
                }
            }
            _ => {
                return Err(server_error(format!(
                    "ERR unknown subcommand 'XGROUP {sub}'; the in-process Redis does not model it"
                )));
            }
        };
        self.recount_group(server, &key, &group_name);
        Ok(reply)
    }

    /// `XREADGROUP GROUP g c [COUNT n] [BLOCK ms] [NOACK] [CLAIM min-idle] STREAMS key.. id..`.
    ///
    /// The read never blocks: the subscription waited for something to read before it issued
    /// it. With `CLAIM` every entry carries its idle time and its delivery count, the pending
    /// entries idle at least `min-idle` first.
    pub(super) fn xreadgroup(
        &mut self,
        server: &Server,
        args: &mut Args<'_>,
        now: Instant,
    ) -> Result<Value, Error> {
        if !args.word("GROUP") {
            return Err(syntax());
        }
        let group_name = args.bytes()?;
        let consumer = args.bytes()?;
        let (mut count, mut noack, mut claim) = (usize::MAX, false, None);
        loop {
            if args.word("COUNT") {
                count = usize::try_from(args.int()?).unwrap_or(0);
            } else if args.word("BLOCK") {
                args.int()?;
            } else if args.word("NOACK") {
                noack = true;
            } else if args.word("CLAIM") {
                claim = Some(Duration::from_millis(args.int()?.unsigned_abs()));
            } else if args.word("STREAMS") {
                break;
            } else {
                return Err(syntax());
            }
        }
        let rest = args.rest()?;
        if rest.is_empty() || rest.len() % 2 != 0 {
            return Err(server_error(
                "ERR Unbalanced 'xreadgroup' list of streams: for each stream key an ID or '>' \
                 must be specified.",
            ));
        }
        let (keys, ids) = rest.split_at(rest.len() / 2);
        let mut replies = Vec::new();
        for (key, id) in keys.iter().zip(ids) {
            let entries = self.read_group(
                server,
                key,
                &group_name,
                &consumer,
                id,
                count,
                noack,
                claim,
                now,
            )?;
            if !entries.is_empty() {
                replies.push(Value::Array(vec![bulk(key.clone()), Value::Array(entries)]));
            }
        }
        Ok(if replies.is_empty() {
            Value::Null
        } else {
            Value::Array(replies)
        })
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the options of one XREADGROUP, read once"
    )]
    fn read_group(
        &mut self,
        server: &Server,
        key: &Bytes,
        group_name: &Bytes,
        consumer: &Bytes,
        id: &Bytes,
        count: usize,
        noack: bool,
        claim: Option<Duration>,
        now: Instant,
    ) -> Result<Vec<Value>, Error> {
        let stream = self
            .keys
            .stream_mut(key, now, false)?
            .ok_or_else(|| no_group(key, group_name, "XREADGROUP"))?;
        let group = stream
            .groups
            .get_mut(group_name)
            .ok_or_else(|| no_group(key, group_name, "XREADGROUP"))?;
        group.consumers.insert(consumer.clone());
        if id.as_ref() != b">" {
            // The consumer's own history: its pending entries past `id`.
            let after: Id = String::from_utf8_lossy(id).parse()?;
            return Ok(group
                .pending
                .range(after.next()..)
                .filter(|(_, pending)| pending.consumer == *consumer)
                .take(count)
                .map(|(id, _)| {
                    stream.entries.get(id).map_or_else(
                        || Value::Array(vec![id.text(), Value::Null]),
                        |fields| Value::Array(entry_value(*id, fields)),
                    )
                })
                .collect());
        }
        let mut replies = Vec::new();
        let mut claimed = Vec::new();
        if let Some(min_idle) = claim {
            let mut stale: Vec<(Id, Duration)> = group
                .pending
                .iter()
                .map(|(id, pending)| (*id, now.duration_since(pending.at)))
                .filter(|(_, idle)| *idle >= min_idle)
                .collect();
            // The longest idle first.
            stale.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
            for (id, idle) in stale.into_iter().take(count) {
                let Some(fields) = stream.entries.get(&id) else {
                    group.pending.remove(&id);
                    continue;
                };
                let pending = group.pending.get_mut(&id).expect("listed above");
                let before = pending.count;
                pending.count += 1;
                pending.at = now;
                pending.consumer = consumer.clone();
                let mut entry = entry_value(id, fields);
                entry.push(Value::Integer(millis(idle)));
                entry.push(Value::Integer(i64::try_from(before).unwrap_or(i64::MAX)));
                replies.push(Value::Array(entry));
                claimed.push(id);
            }
        }
        let mut delivered = Vec::new();
        let fresh: Vec<(Id, Fields)> = stream
            .entries
            .range(group.cursor.next()..)
            .take(count.saturating_sub(replies.len()))
            .map(|(id, fields)| (*id, Arc::clone(fields)))
            .collect();
        for (id, fields) in fresh {
            group.cursor = id;
            if !noack {
                group.pending.insert(
                    id,
                    Pending {
                        consumer: consumer.clone(),
                        at: now,
                        count: 1,
                    },
                );
            }
            let mut entry = entry_value(id, &fields);
            if claim.is_some() {
                entry.push(Value::Integer(0));
                entry.push(Value::Integer(0));
            }
            replies.push(Value::Array(entry));
            delivered.push(id);
        }
        self.handed_out(
            server, key, group_name, consumer, &claimed, &delivered, noack, now,
        );
        Ok(replies)
    }

    /// Counts the entries a read handed out: a registered subscription's delivery takes over the
    /// entry's count, and on a group that claims each one is timed to come due again.
    #[allow(clippy::too_many_arguments, reason = "one read's hand-out, told once")]
    fn handed_out(
        &mut self,
        server: &Server,
        key: &Bytes,
        group: &Bytes,
        consumer: &Bytes,
        claimed: &[Id],
        delivered: &[Id],
        noack: bool,
        now: Instant,
    ) {
        let registered = self.registered(key, group, consumer);
        let slot = (key.clone(), group.clone());
        for id in claimed.iter().chain(delivered) {
            if let Some(due) = self.claim_due.get_mut(&slot) {
                due.remove(id);
            }
            let owed = self
                .stream_owed
                .get_mut(&slot)
                .is_some_and(|owed| owed.remove(id));
            match (owed, registered) {
                (false, true) => server.owe(1),
                (true, false) => server.released(1),
                _ => {}
            }
        }
        let Some(min_idle) = self.group_readers(key, group).claim else {
            return;
        };
        if noack {
            return;
        }
        for id in claimed.iter().chain(delivered).copied() {
            let (key, group) = (key.clone(), group.clone());
            server.schedule(min_idle, move |server| {
                {
                    let mut state = server.lock();
                    state.claim_came_due(server, &key, &group, id, now);
                }
                server.changed.notify_waiters();
            });
        }
    }

    /// Times every entry pending in `group` to come due for a reader that claims at `min_idle`:
    /// one idle long enough already is due now.
    pub(super) fn arm_claims(
        &mut self,
        server: &Server,
        key: &Bytes,
        group: &Bytes,
        min_idle: Duration,
    ) {
        let now = Server::now();
        let pending: Vec<(Id, Instant)> = self
            .keys
            .stream(key, now)
            .ok()
            .flatten()
            .and_then(|stream| stream.groups.get(group))
            .map(|group| {
                group
                    .pending
                    .iter()
                    .map(|(id, pending)| (*id, pending.at))
                    .collect()
            })
            .unwrap_or_default();
        for (id, at) in pending {
            let idle = now.duration_since(at);
            if idle >= min_idle {
                self.claim_came_due(server, key, group, id, at);
            } else {
                let (key, group) = (key.clone(), group.clone());
                server.schedule(min_idle.saturating_sub(idle), move |server| {
                    {
                        let mut state = server.lock();
                        state.claim_came_due(server, &key, &group, id, at);
                    }
                    server.changed.notify_waiters();
                });
            }
        }
    }

    /// Marks a pending entry claimable, when it is still pending from the delivery at `at`, and
    /// counts the delivery a claiming reader owes it.
    fn claim_came_due(&mut self, server: &Server, key: &Bytes, group: &Bytes, id: Id, at: Instant) {
        let now = Server::now();
        let still = self
            .keys
            .stream(key, now)
            .ok()
            .flatten()
            .and_then(|stream| stream.groups.get(group))
            .and_then(|group| group.pending.get(&id))
            .is_some_and(|pending| pending.at == at);
        if !still {
            return;
        }
        let slot = (key.clone(), group.clone());
        self.claim_due.entry(slot.clone()).or_default().insert(id);
        let readers = self.group_readers(key, group);
        if readers.any
            && readers.claim.is_some()
            && self.stream_owed.entry(slot).or_default().insert(id)
        {
            server.owe(1);
        }
    }

    /// `XAUTOCLAIM key group consumer min-idle start [COUNT n] [JUSTID]`.
    pub(super) fn xautoclaim(
        &mut self,
        server: &Server,
        args: &mut Args<'_>,
        now: Instant,
    ) -> Result<Value, Error> {
        let key = args.bytes()?;
        let group_name = args.bytes()?;
        let consumer = args.bytes()?;
        let min_idle = Duration::from_millis(args.int()?.unsigned_abs());
        let start: Id = args.text()?.parse()?;
        let mut count = 100;
        let mut just_id = false;
        while args.left() > 0 {
            if args.word("COUNT") {
                count = usize::try_from(args.int()?).unwrap_or(0);
            } else if args.word("JUSTID") {
                just_id = true;
            } else {
                return Err(syntax());
            }
        }
        let stream = self
            .keys
            .stream_mut(&key, now, false)?
            .ok_or_else(|| no_group(&key, &group_name, "XAUTOCLAIM"))?;
        let group = stream
            .groups
            .get_mut(&group_name)
            .ok_or_else(|| no_group(&key, &group_name, "XAUTOCLAIM"))?;
        group.consumers.insert(consumer.clone());
        let candidates: Vec<Id> = group
            .pending
            .range(start..)
            .filter(|(_, pending)| now.duration_since(pending.at) >= min_idle)
            .map(|(id, _)| *id)
            .collect();
        let mut claimed = Vec::new();
        let mut replies = Vec::new();
        let mut deleted = Vec::new();
        let mut next = Id::default();
        for id in candidates {
            if claimed.len() >= count {
                next = id;
                break;
            }
            let Some(fields) = stream.entries.get(&id) else {
                group.pending.remove(&id);
                deleted.push(id.text());
                continue;
            };
            let pending = group.pending.get_mut(&id).expect("listed above");
            pending.count += 1;
            pending.at = now;
            pending.consumer = consumer.clone();
            replies.push(if just_id {
                id.text()
            } else {
                Value::Array(entry_value(id, fields))
            });
            claimed.push(id);
        }
        self.handed_out(
            server,
            &key,
            &group_name,
            &consumer,
            &claimed,
            &[],
            false,
            now,
        );
        Ok(Value::Array(vec![
            next.text(),
            Value::Array(replies),
            Value::Array(deleted),
        ]))
    }

    /// `XPENDING key group`, or the extended form `XPENDING key group [IDLE min] start end count
    /// [consumer]`.
    pub(super) fn xpending(&self, args: &mut Args<'_>, now: Instant) -> Result<Value, Error> {
        let key = args.bytes()?;
        let group_name = args.bytes()?;
        let group = self
            .keys
            .stream(&key, now)?
            .and_then(|stream| stream.groups.get(&group_name))
            .ok_or_else(|| no_group(&key, &group_name, "XPENDING"))?;
        if args.left() == 0 {
            let mut per_consumer: BTreeMap<Bytes, usize> = BTreeMap::new();
            for pending in group.pending.values() {
                *per_consumer.entry(pending.consumer.clone()).or_default() += 1;
            }
            let first = group.pending.keys().next();
            let last = group.pending.keys().next_back();
            return Ok(Value::Array(vec![
                int(group.pending.len()),
                first.map_or(Value::Null, |id| id.text()),
                last.map_or(Value::Null, |id| id.text()),
                if per_consumer.is_empty() {
                    Value::Null
                } else {
                    Value::Array(
                        per_consumer
                            .into_iter()
                            .map(|(consumer, n)| {
                                Value::Array(vec![
                                    bulk(consumer),
                                    Value::String(n.to_string().as_str().into()),
                                ])
                            })
                            .collect(),
                    )
                },
            ]));
        }
        let min_idle = if args.word("IDLE") {
            Duration::from_millis(args.int()?.unsigned_abs())
        } else {
            Duration::ZERO
        };
        let start = range_end(&args.text()?, Id::default(), true)?;
        let end = range_end(&args.text()?, Id::MAX, false)?;
        let count = usize::try_from(args.int()?).unwrap_or(0);
        let consumer = if args.left() > 0 {
            Some(args.bytes()?)
        } else {
            None
        };
        if start > end {
            return Ok(Value::Array(Vec::new()));
        }
        Ok(Value::Array(
            group
                .pending
                .range(start..=end)
                .filter(|(_, pending)| {
                    consumer
                        .as_ref()
                        .is_none_or(|name| pending.consumer == *name)
                        && now.duration_since(pending.at) >= min_idle
                })
                .take(count)
                .map(|(id, pending)| {
                    Value::Array(vec![
                        id.text(),
                        bulk(pending.consumer.clone()),
                        Value::Integer(millis(now.duration_since(pending.at))),
                        Value::Integer(i64::try_from(pending.count).unwrap_or(i64::MAX)),
                    ])
                })
                .collect(),
        ))
    }
}

/// One end of an `XRANGE` or `XPENDING` range: `-`, `+`, an id, or `(id` for an exclusive end.
fn range_end(text: &str, open: Id, start: bool) -> Result<Id, Error> {
    match text {
        "-" | "+" => Ok(open),
        text => {
            if let Some(id) = text.strip_prefix('(') {
                let id: Id = id.parse()?;
                return Ok(if start { id.next() } else { previous(id) });
            }
            let id: Id = text.parse()?;
            // An end given as milliseconds alone takes every sequence of that millisecond.
            Ok(if !start && !text.contains('-') {
                Id {
                    ms: id.ms,
                    seq: u64::MAX,
                }
            } else {
                id
            })
        }
    }
}

const fn previous(id: Id) -> Id {
    if id.seq > 0 {
        Id {
            ms: id.ms,
            seq: id.seq - 1,
        }
    } else {
        Id {
            ms: id.ms.saturating_sub(1),
            seq: u64::MAX,
        }
    }
}
