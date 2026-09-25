//! The keyspace of the in-process server and the commands on every key type but the stream.

use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::time::Duration;

use bytes::Bytes;
use fred::error::Error;
use fred::mocks::MockCommand;
use fred::types::Value;
use fred::util::redis_keyslot;
use tokio::time::Instant;

use super::args::{
    Args, bulk, float_text, int, not_integer, ok, server_error, syntax, text_of, wrong_type,
};
use super::streams::Stream;
use super::{Server, State, VERSION};

/// One write to a name, as the harness's `published` reads it back.
#[derive(Clone, Debug)]
pub(crate) enum Write {
    /// An `XADD`: the entry's fields.
    Fields(Vec<(Bytes, Bytes)>),
    /// An `LPUSH`, an `RPUSH` or a `PUBLISH`: the framed body.
    Body(Bytes),
}

/// A delay queue or a recovery set a subscription's sweep reads, and the stream or list it moves
/// its members back onto.
#[derive(Debug)]
pub(crate) struct Link {
    pub(crate) target: Bytes,
    pub(crate) kind: LinkKind,
    /// Members whose wait is over, which a sweep moves now.
    pub(crate) due: BTreeSet<Bytes>,
    /// Due members counted in flight with the harness.
    pub(crate) held: BTreeSet<Bytes>,
}

impl Link {
    pub(crate) const fn new(target: Bytes, kind: LinkKind) -> Self {
        Self {
            target,
            kind,
            due: BTreeSet::new(),
            held: BTreeSet::new(),
        }
    }
}

/// What a linked set's score means.
#[derive(Debug, Clone, Copy)]
pub(crate) enum LinkKind {
    /// A delay queue: the score is when the member is due.
    Delay,
    /// A list's recovery set: the score is when the entry was claimed, and it is stranded once it
    /// has been idle this long.
    Recovery(Duration),
}

/// A value of the keyspace.
#[derive(Debug)]
pub(crate) enum Data {
    Str(Bytes),
    List(VecDeque<Bytes>),
    Hash(HashMap<Bytes, Bytes>),
    Set(HashSet<Bytes>),
    ZSet(HashMap<Bytes, f64>),
    Stream(Stream),
}

impl Data {
    const fn type_name(&self) -> &'static str {
        match self {
            Self::Str(_) => "string",
            Self::List(_) => "list",
            Self::Hash(_) => "hash",
            Self::Set(_) => "set",
            Self::ZSet(_) => "zset",
            Self::Stream(_) => "stream",
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Self::Str(_) | Self::Stream(_) => false,
            Self::List(list) => list.is_empty(),
            Self::Hash(hash) => hash.is_empty(),
            Self::Set(set) => set.is_empty(),
            Self::ZSet(zset) => zset.is_empty(),
        }
    }
}

#[derive(Debug)]
struct Entry {
    data: Data,
    expires: Option<Instant>,
}

/// The server's keys.
#[derive(Debug, Default)]
pub(crate) struct Keyspace {
    map: HashMap<Bytes, Entry>,
}

/// Generates the typed accessors of one value type: a read that treats an expired key as absent,
/// and a write that drops an expired key and creates the value when asked to.
macro_rules! typed {
    ($read:ident, $write:ident, $variant:ident, $ty:ty) => {
        pub(crate) fn $read(&self, key: &[u8], now: Instant) -> Result<Option<&$ty>, Error> {
            match self.live(key, now) {
                None => Ok(None),
                Some(Data::$variant(value)) => Ok(Some(value)),
                Some(_) => Err(wrong_type()),
            }
        }

        #[allow(dead_code, reason = "not every type is written through its accessor")]
        pub(crate) fn $write(
            &mut self,
            key: &Bytes,
            now: Instant,
            create: bool,
        ) -> Result<Option<&mut $ty>, Error> {
            self.purge(key, now);
            if create && !self.map.contains_key(key) {
                self.map.insert(
                    key.clone(),
                    Entry {
                        data: Data::$variant(Default::default()),
                        expires: None,
                    },
                );
            }
            match self.map.get_mut(key) {
                None => Ok(None),
                Some(Entry {
                    data: Data::$variant(value),
                    ..
                }) => Ok(Some(value)),
                Some(_) => Err(wrong_type()),
            }
        }
    };
}

impl Keyspace {
    fn live(&self, key: &[u8], now: Instant) -> Option<&Data> {
        self.map
            .get(key)
            .filter(|entry| entry.expires.is_none_or(|at| at > now))
            .map(|entry| &entry.data)
    }

    /// Drops `key` when it has expired.
    fn purge(&mut self, key: &[u8], now: Instant) {
        if self
            .map
            .get(key)
            .is_some_and(|entry| entry.expires.is_some_and(|at| at <= now))
        {
            self.map.remove(key);
        }
    }

    /// Drops `key` when a write left its collection empty, as the server does.
    pub(crate) fn drop_empty(&mut self, key: &[u8]) {
        if self.map.get(key).is_some_and(|entry| entry.data.is_empty()) {
            self.map.remove(key);
        }
    }

    pub(crate) fn remove(&mut self, key: &[u8], now: Instant) -> Option<Data> {
        self.purge(key, now);
        self.map.remove(key).map(|entry| entry.data)
    }

    pub(crate) fn exists(&self, key: &[u8], now: Instant) -> bool {
        self.live(key, now).is_some()
    }

    typed!(string, string_mut, Str, Bytes);
    typed!(list, list_mut, List, VecDeque<Bytes>);
    typed!(hash, hash_mut, Hash, HashMap<Bytes, Bytes>);
    typed!(set, set_mut, Set, HashSet<Bytes>);
    typed!(zset, zset_mut, ZSet, HashMap<Bytes, f64>);
    typed!(stream, stream_mut, Stream, Stream);

    pub(crate) fn list_len(&self, key: &[u8], now: Instant) -> usize {
        self.list(key, now).ok().flatten().map_or(0, VecDeque::len)
    }

    pub(crate) fn zscore(&self, key: &[u8], member: &[u8], now: Instant) -> Option<f64> {
        self.zset(key, now)
            .ok()
            .flatten()
            .and_then(|zset| zset.get(member).copied())
    }

    fn set_string(&mut self, key: Bytes, value: Bytes, expires: Option<Instant>) {
        self.map.insert(
            key,
            Entry {
                data: Data::Str(value),
                expires,
            },
        );
    }

    fn expires_mut(&mut self, key: &[u8], now: Instant) -> Option<&mut Option<Instant>> {
        self.purge(key, now);
        self.map.get_mut(key).map(|entry| &mut entry.expires)
    }

    fn type_of(&self, key: &[u8], now: Instant) -> &'static str {
        self.live(key, now).map_or("none", Data::type_name)
    }
}

impl State {
    pub(super) fn dispatch(
        &mut self,
        server: &Server,
        name: &str,
        sub: Option<&str>,
        values: &[Value],
    ) -> Result<Value, Error> {
        let now = Server::now();
        let mut args = Args::new(name, values);
        let families = [
            Self::server_commands,
            Self::keys_and_strings,
            Self::hashes,
            Self::sets,
            Self::lists,
            Self::list_reads,
            Self::sorted_sets,
        ];
        for family in families {
            if let Some(reply) = family(self, server, name, &mut args, now)? {
                return Ok(reply);
            }
        }
        match (name, sub) {
            ("XADD", _) => self.xadd(server, &mut args, now),
            ("XLEN", _) => self.xlen(&mut args, now),
            ("XRANGE" | "XREVRANGE", _) => self.xrange(&mut args, now, name == "XREVRANGE"),
            ("XDEL", _) => self.xdel(server, &mut args, now),
            ("XTRIM", _) => self.xtrim(server, &mut args, now),
            ("XACK", _) => self.xack(server, &mut args, now),
            ("XREADGROUP", _) => self.xreadgroup(server, &mut args, now),
            ("XAUTOCLAIM", _) => self.xautoclaim(server, &mut args, now),
            ("XPENDING", _) => self.xpending(&mut args, now),
            ("XGROUP", Some(sub)) => self.xgroup(server, sub, &mut args, now),
            _ => Err(server_error(format!(
                "ERR unknown command '{}': the in-process Redis does not model it; test this \
                 handler against a real server",
                sub.map_or_else(|| name.to_owned(), |sub| format!("{name} {sub}"))
            ))),
        }
    }

    /// The connection, Pub/Sub and scripting commands.
    #[allow(
        clippy::unnecessary_wraps,
        reason = "every family answers the same way"
    )]
    fn server_commands(
        &mut self,
        server: &Server,
        name: &str,
        args: &mut Args<'_>,
        now: Instant,
    ) -> Result<Option<Value>, Error> {
        let _ = (server, now);
        let reply = match name {
            "PING" => Ok(match args.left() {
                0 => Value::String("PONG".into()),
                _ => bulk(args.bytes()?),
            }),
            "ECHO" => Ok(bulk(args.bytes()?)),
            "QUIT" | "RESET" | "SELECT" | "CLIENT" | "READONLY" | "READWRITE" | "UNSUBSCRIBE"
            | "SUNSUBSCRIBE" | "PUNSUBSCRIBE" => Ok(ok()),
            "INFO" => Ok(bulk(Bytes::from(format!(
                "# Server\r\nredis_version:{}.{}.{}\r\n",
                VERSION.major, VERSION.minor, VERSION.patch
            )))),
            "SUBSCRIBE" | "SSUBSCRIBE" | "PSUBSCRIBE" => {
                // The subscription itself is registered by the subscriber that issued this.
                let kind = name.to_ascii_lowercase();
                Ok(Value::Array(
                    args.rest()?
                        .into_iter()
                        .enumerate()
                        .flat_map(|(at, channel)| {
                            [
                                Value::String(kind.as_str().into()),
                                bulk(channel),
                                int(at + 1),
                            ]
                        })
                        .collect(),
                ))
            }
            "PUBLISH" | "SPUBLISH" => {
                let channel = args.bytes()?;
                let body = args.bytes()?;
                args.done()?;
                Ok(int(self.publish(
                    server,
                    &channel,
                    &body,
                    name == "SPUBLISH",
                )))
            }
            "EVAL" | "EVALSHA" | "EVAL_RO" | "EVALSHA_RO" | "FCALL" | "FCALL_RO" | "SCRIPT"
            | "FUNCTION" => Err(server_error(format!(
                "ERR {name} is not available in process: the in-process Redis runs no scripts or \
                 functions; test this handler against a real server"
            ))),
            _ => return Ok(None),
        };
        reply.map(Some)
    }

    /// The keyspace commands and the string commands.
    #[allow(
        clippy::unnecessary_wraps,
        reason = "every family answers the same way"
    )]
    fn keys_and_strings(
        &mut self,
        server: &Server,
        name: &str,
        args: &mut Args<'_>,
        now: Instant,
    ) -> Result<Option<Value>, Error> {
        let _ = (server, now);
        let reply = match name {
            "DEL" | "UNLINK" => {
                let keys = args.rest()?;
                if keys.is_empty() {
                    return Err(args.arity());
                }
                Ok(int(keys
                    .iter()
                    .filter(|key| self.delete(server, key))
                    .count()))
            }
            "EXISTS" => {
                let keys = args.rest()?;
                Ok(int(keys
                    .iter()
                    .filter(|key| self.keys.exists(key, now))
                    .count()))
            }
            "TYPE" => {
                let key = args.bytes()?;
                Ok(Value::String(self.keys.type_of(&key, now).into()))
            }
            "EXPIRE" | "PEXPIRE" => {
                let key = args.bytes()?;
                let amount = args.int()?;
                let millis = if name == "EXPIRE" {
                    amount.saturating_mul(1000)
                } else {
                    amount
                };
                Ok(int(usize::from(self.expire(server, &key, millis, now))))
            }
            "PERSIST" => {
                let key = args.bytes()?;
                Ok(int(usize::from(
                    self.keys
                        .expires_mut(&key, now)
                        .is_some_and(|expires| expires.take().is_some()),
                )))
            }
            "TTL" | "PTTL" => {
                let key = args.bytes()?;
                Ok(Value::Integer(match self.keys.expires_mut(&key, now) {
                    None => -2,
                    Some(None) => -1,
                    Some(Some(at)) => {
                        let left = at.duration_since(now);
                        let left = if name == "TTL" {
                            left.as_secs()
                        } else {
                            u64::try_from(left.as_millis()).unwrap_or(u64::MAX)
                        };
                        i64::try_from(left).unwrap_or(i64::MAX)
                    }
                }))
            }
            "FLUSHALL" | "FLUSHDB" => {
                let keys: Vec<Bytes> = self.keys.map.keys().cloned().collect();
                for key in keys {
                    self.delete(server, &key);
                }
                Ok(ok())
            }
            "GET" => {
                let key = args.bytes()?;
                Ok(self
                    .keys
                    .string(&key, now)?
                    .map_or(Value::Null, |value| bulk(value.clone())))
            }
            "SET" => self.set(args, now),
            "INCR" | "DECR" | "INCRBY" | "DECRBY" => {
                let key = args.bytes()?;
                let by = match name {
                    "INCR" => 1,
                    "DECR" => -1,
                    "INCRBY" => args.int()?,
                    _ => args.int()?.checked_neg().ok_or_else(not_integer)?,
                };
                args.done()?;
                let current = match self.keys.string(&key, now)? {
                    Some(value) => text_of(&bulk(value.clone()))?
                        .parse::<i64>()
                        .map_err(|_| not_integer())?,
                    None => 0,
                };
                let next = current.checked_add(by).ok_or_else(not_integer)?;
                let expires = self.keys.expires_mut(&key, now).and_then(|at| *at);
                self.keys
                    .set_string(key, Bytes::from(next.to_string()), expires);
                Ok(Value::Integer(next))
            }
            _ => return Ok(None),
        };
        reply.map(Some)
    }

    /// The hash commands.
    #[allow(
        clippy::unnecessary_wraps,
        reason = "every family answers the same way"
    )]
    fn hashes(
        &mut self,
        server: &Server,
        name: &str,
        args: &mut Args<'_>,
        now: Instant,
    ) -> Result<Option<Value>, Error> {
        let _ = (server, now);
        let reply = match name {
            "HSET" | "HMSET" => {
                let key = args.bytes()?;
                let pairs = args.rest()?;
                if pairs.is_empty() || pairs.len() % 2 != 0 {
                    return Err(args.arity());
                }
                let hash = self.keys.hash_mut(&key, now, true)?.expect("created");
                let added = pairs
                    .chunks(2)
                    .filter(|pair| hash.insert(pair[0].clone(), pair[1].clone()).is_none())
                    .count();
                Ok(if name == "HMSET" { ok() } else { int(added) })
            }
            "HGET" => {
                let key = args.bytes()?;
                let field = args.bytes()?;
                Ok(self
                    .keys
                    .hash(&key, now)?
                    .and_then(|hash| hash.get(&field))
                    .map_or(Value::Null, |value| bulk(value.clone())))
            }
            "HGETALL" => {
                let key = args.bytes()?;
                Ok(Value::Array(
                    self.keys
                        .hash(&key, now)?
                        .into_iter()
                        .flatten()
                        .flat_map(|(field, value)| [bulk(field.clone()), bulk(value.clone())])
                        .collect(),
                ))
            }
            "HDEL" => {
                let key = args.bytes()?;
                let fields = args.rest()?;
                let removed = self.keys.hash_mut(&key, now, false)?.map_or(0, |hash| {
                    fields
                        .iter()
                        .filter(|field| hash.remove(*field).is_some())
                        .count()
                });
                self.keys.drop_empty(&key);
                Ok(int(removed))
            }
            "HEXISTS" => {
                let key = args.bytes()?;
                let field = args.bytes()?;
                Ok(int(usize::from(
                    self.keys
                        .hash(&key, now)?
                        .is_some_and(|hash| hash.contains_key(&field)),
                )))
            }
            "HLEN" => {
                let key = args.bytes()?;
                Ok(int(self.keys.hash(&key, now)?.map_or(0, HashMap::len)))
            }
            "HINCRBY" => {
                let key = args.bytes()?;
                let field = args.bytes()?;
                let by = args.int()?;
                let hash = self.keys.hash_mut(&key, now, true)?.expect("created");
                let current = match hash.get(&field) {
                    Some(value) => text_of(&bulk(value.clone()))?
                        .parse::<i64>()
                        .map_err(|_| not_integer())?,
                    None => 0,
                };
                let next = current.checked_add(by).ok_or_else(not_integer)?;
                hash.insert(field, Bytes::from(next.to_string()));
                Ok(Value::Integer(next))
            }
            _ => return Ok(None),
        };
        reply.map(Some)
    }

    /// The set commands.
    #[allow(
        clippy::unnecessary_wraps,
        reason = "every family answers the same way"
    )]
    fn sets(
        &mut self,
        server: &Server,
        name: &str,
        args: &mut Args<'_>,
        now: Instant,
    ) -> Result<Option<Value>, Error> {
        let _ = (server, now);
        let reply = match name {
            "SADD" => {
                let key = args.bytes()?;
                let members = args.rest()?;
                let set = self.keys.set_mut(&key, now, true)?.expect("created");
                Ok(int(members
                    .into_iter()
                    .filter(|member| set.insert(member.clone()))
                    .count()))
            }
            "SREM" => {
                let key = args.bytes()?;
                let members = args.rest()?;
                let removed = self.keys.set_mut(&key, now, false)?.map_or(0, |set| {
                    members.iter().filter(|member| set.remove(*member)).count()
                });
                self.keys.drop_empty(&key);
                Ok(int(removed))
            }
            "SMEMBERS" => {
                let key = args.bytes()?;
                Ok(Value::Array(
                    self.keys
                        .set(&key, now)?
                        .into_iter()
                        .flatten()
                        .map(|member| bulk(member.clone()))
                        .collect(),
                ))
            }
            "SISMEMBER" => {
                let key = args.bytes()?;
                let member = args.bytes()?;
                Ok(int(usize::from(
                    self.keys
                        .set(&key, now)?
                        .is_some_and(|set| set.contains(&member)),
                )))
            }
            "SCARD" => {
                let key = args.bytes()?;
                Ok(int(self.keys.set(&key, now)?.map_or(0, HashSet::len)))
            }
            _ => return Ok(None),
        };
        reply.map(Some)
    }

    /// The sorted-set commands.
    #[allow(
        clippy::unnecessary_wraps,
        reason = "every family answers the same way"
    )]
    fn sorted_sets(
        &mut self,
        server: &Server,
        name: &str,
        args: &mut Args<'_>,
        now: Instant,
    ) -> Result<Option<Value>, Error> {
        let _ = (server, now);
        let reply = match name {
            "ZADD" => self.zadd(server, args, now),
            "ZREM" => {
                let key = args.bytes()?;
                let members = args.rest()?;
                let removed: Vec<Bytes> =
                    self.keys
                        .zset_mut(&key, now, false)?
                        .map_or_else(Vec::new, |zset| {
                            members
                                .into_iter()
                                .filter(|member| zset.remove(member).is_some())
                                .collect()
                        });
                self.keys.drop_empty(&key);
                for member in &removed {
                    self.link_removed(server, &key, member);
                }
                Ok(int(removed.len()))
            }
            "ZSCORE" => {
                let key = args.bytes()?;
                let member = args.bytes()?;
                Ok(self
                    .keys
                    .zset(&key, now)?
                    .and_then(|zset| zset.get(&member))
                    .map_or(Value::Null, |score| bulk(Bytes::from(float_text(*score)))))
            }
            "ZCARD" => {
                let key = args.bytes()?;
                Ok(int(self.keys.zset(&key, now)?.map_or(0, HashMap::len)))
            }
            "ZRANGEBYSCORE" => self.zrangebyscore(args, now),
            "ZREMRANGEBYSCORE" => {
                let key = args.bytes()?;
                let (min, max) = (Bound::parse(args)?, Bound::parse(args)?);
                args.done()?;
                let removed: Vec<Bytes> =
                    self.keys
                        .zset_mut(&key, now, false)?
                        .map_or_else(Vec::new, |zset| {
                            let gone: Vec<Bytes> = zset
                                .iter()
                                .filter(|(_, score)| min.below(**score) && max.above(**score))
                                .map(|(member, _)| member.clone())
                                .collect();
                            for member in &gone {
                                zset.remove(member);
                            }
                            gone
                        });
                self.keys.drop_empty(&key);
                for member in &removed {
                    self.link_removed(server, &key, member);
                }
                Ok(int(removed.len()))
            }
            _ => return Ok(None),
        };
        reply.map(Some)
    }

    /// Removes `key`, and whatever the model tracked for it.
    fn delete(&mut self, server: &Server, key: &Bytes) -> bool {
        let now = Server::now();
        let Some(data) = self.keys.remove(key, now) else {
            return false;
        };
        match data {
            Data::List(_) => self.recount_list(server, key),
            Data::ZSet(members) => {
                for member in members.keys() {
                    self.link_removed(server, key, member);
                }
            }
            Data::Stream(stream) => {
                for group in stream.groups() {
                    self.claim_due.remove(&(key.clone(), group.clone()));
                    self.recount_group(server, key, &group);
                }
            }
            Data::Str(_) | Data::Hash(_) | Data::Set(_) => {}
        }
        true
    }

    fn expire(&mut self, server: &Server, key: &Bytes, millis: i64, now: Instant) -> bool {
        if millis <= 0 {
            return self.delete(server, key);
        }
        let at = now + Duration::from_millis(millis.unsigned_abs());
        self.keys.expires_mut(key, now).is_some_and(|expires| {
            *expires = Some(at);
            true
        })
    }

    fn set(&mut self, args: &mut Args<'_>, now: Instant) -> Result<Value, Error> {
        let key = args.bytes()?;
        let value = args.bytes()?;
        let (mut only_new, mut only_old, mut get) = (false, false, false);
        let mut expires = None;
        let mut keep_ttl = false;
        while args.left() > 0 {
            if args.word("NX") {
                only_new = true;
            } else if args.word("XX") {
                only_old = true;
            } else if args.word("GET") {
                get = true;
            } else if args.word("KEEPTTL") {
                keep_ttl = true;
            } else if args.word("EX") {
                expires = Some(now + Duration::from_secs(args.int()?.unsigned_abs()));
            } else if args.word("PX") {
                expires = Some(now + Duration::from_millis(args.int()?.unsigned_abs()));
            } else {
                return Err(syntax());
            }
        }
        let old = self.keys.string(&key, now)?.cloned();
        let exists = self.keys.exists(&key, now);
        let reply = if get {
            old.map_or(Value::Null, bulk)
        } else {
            ok()
        };
        if (only_new && exists) || (only_old && !exists) {
            return Ok(if get { reply } else { Value::Null });
        }
        if keep_ttl {
            expires = self.keys.expires_mut(&key, now).and_then(|at| *at);
        }
        self.keys.set_string(key, value, expires);
        Ok(reply)
    }

    fn zadd(&mut self, server: &Server, args: &mut Args<'_>, now: Instant) -> Result<Value, Error> {
        let key = args.bytes()?;
        let (mut only_new, mut only_old, mut changed) = (false, false, false);
        loop {
            if args.word("NX") {
                only_new = true;
            } else if args.word("XX") {
                only_old = true;
            } else if args.word("CH") {
                changed = true;
            } else if args.word("GT") || args.word("LT") {
                // Neither changes what a member added here scores.
            } else if args.word("INCR") {
                return Err(server_error(
                    "ERR ZADD INCR is not available in process; use ZINCRBY",
                ));
            } else {
                break;
            }
        }
        let mut pairs = Vec::new();
        while args.left() > 0 {
            let score = args.float()?;
            let member = args.bytes()?;
            pairs.push((score, member));
        }
        if pairs.is_empty() {
            return Err(args.arity());
        }
        let zset = self.keys.zset_mut(&key, now, true)?.expect("created");
        let mut added = 0;
        let mut updated = 0;
        let mut written = Vec::new();
        for (score, member) in pairs {
            match zset.get(&member).copied() {
                None if !only_old => {
                    zset.insert(member.clone(), score);
                    added += 1;
                    written.push((member, score));
                }
                #[allow(clippy::float_cmp, reason = "an unchanged score is no update")]
                Some(held) if !only_new && held != score => {
                    zset.insert(member.clone(), score);
                    updated += 1;
                    written.push((member, score));
                }
                _ => {}
            }
        }
        self.keys.drop_empty(&key);
        for (member, score) in written {
            self.arm_link(server, &key, &member, score);
        }
        Ok(int(if changed { added + updated } else { added }))
    }

    fn zrangebyscore(&self, args: &mut Args<'_>, now: Instant) -> Result<Value, Error> {
        let key = args.bytes()?;
        let (min, max) = (Bound::parse(args)?, Bound::parse(args)?);
        let (mut scores, mut limit) = (false, None);
        while args.left() > 0 {
            if args.word("WITHSCORES") {
                scores = true;
            } else if args.word("LIMIT") {
                let offset = usize::try_from(args.int()?).unwrap_or(0);
                let count = args.int()?;
                limit = Some((offset, usize::try_from(count).ok()));
            } else {
                return Err(syntax());
            }
        }
        let mut members: Vec<(f64, Bytes)> = self
            .keys
            .zset(&key, now)?
            .into_iter()
            .flatten()
            .filter(|(_, score)| min.below(**score) && max.above(**score))
            .map(|(member, score)| (*score, member.clone()))
            .collect();
        members.sort_by(|a, b| a.0.total_cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
        let (offset, count) = limit.unwrap_or((0, None));
        Ok(Value::Array(
            members
                .into_iter()
                .skip(offset)
                .take(count.unwrap_or(usize::MAX))
                .flat_map(|(score, member)| {
                    let score = scores.then(|| bulk(Bytes::from(float_text(score))));
                    std::iter::once(bulk(member)).chain(score)
                })
                .collect(),
        ))
    }
}

/// One end of a score range: `-inf`, `+inf`, a score, or `(score` for an exclusive end.
#[derive(Clone, Copy)]
struct Bound {
    score: f64,
    exclusive: bool,
}

impl Bound {
    fn parse(args: &mut Args<'_>) -> Result<Self, Error> {
        let Some(value) = args.peek() else {
            return Err(args.arity());
        };
        if let Value::Double(_) | Value::Integer(_) = value {
            return Ok(Self {
                score: args.float()?,
                exclusive: false,
            });
        }
        let text = args.text()?;
        let (exclusive, score) = text
            .strip_prefix('(')
            .map_or((false, text.as_str()), |rest| (true, rest));
        Ok(Self {
            score: super::args::float_of(&Value::String(score.into()))
                .map_err(|_| server_error("ERR min or max is not a float"))?,
            exclusive,
        })
    }

    /// Whether `score` lies above this lower end.
    fn below(self, score: f64) -> bool {
        if self.exclusive {
            score > self.score
        } else {
            score >= self.score
        }
    }

    /// Whether `score` lies below this upper end.
    fn above(self, score: f64) -> bool {
        if self.exclusive {
            score < self.score
        } else {
            score <= self.score
        }
    }
}

/// The keys a command reads or writes, for the cluster's slot check.
fn keys_of(name: &str, sub: Option<&str>, values: &[Value]) -> Vec<Bytes> {
    let texts = || {
        values
            .iter()
            .filter_map(|value| super::args::bytes_of(value).ok())
    };
    match (name, sub) {
        ("DEL" | "UNLINK" | "EXISTS" | "MGET", _) => texts().collect(),
        ("LMOVE" | "BLMOVE" | "RPOPLPUSH" | "BRPOPLPUSH", _) => texts().take(2).collect(),
        ("BRPOP" | "BLPOP", _) => {
            let keys: Vec<Bytes> = texts().collect();
            keys[..keys.len().saturating_sub(1)].to_vec()
        }
        ("XREADGROUP" | "XREAD", _) => {
            let words: Vec<Bytes> = texts().collect();
            let streams = words
                .iter()
                .position(|word| word.eq_ignore_ascii_case(b"STREAMS"))
                .map_or(0, |at| at + 1);
            let tail = &words[streams.min(words.len())..];
            tail[..tail.len() / 2].to_vec()
        }
        (
            "PING" | "ECHO" | "QUIT" | "RESET" | "SELECT" | "CLIENT" | "INFO" | "PUBLISH"
            | "SUBSCRIBE" | "PSUBSCRIBE" | "UNSUBSCRIBE" | "PUNSUBSCRIBE" | "FLUSHALL" | "FLUSHDB"
            | "READONLY" | "READWRITE" | "MULTI" | "EXEC" | "DISCARD",
            _,
        ) => Vec::new(),
        _ => texts().take(1).collect(),
    }
}

fn crossslot() -> Error {
    server_error("CROSSSLOT Keys in request don't hash to the same slot")
}

/// Refuses a command whose keys live on different slots of a cluster.
pub(crate) fn check_one_slot(name: &str, sub: Option<&str>, values: &[Value]) -> Result<(), Error> {
    let keys = keys_of(name, sub, values);
    let mut slots = keys.iter().map(|key| redis_keyslot(key));
    let first = slots.next();
    if slots.any(|slot| Some(slot) != first) {
        return Err(crossslot());
    }
    Ok(())
}

/// Refuses a transaction whose commands touch keys on different slots of a cluster: the server
/// aborts the `EXEC` rather than run part of it.
pub(crate) fn check_transaction_slot(commands: &[MockCommand]) -> Result<(), Error> {
    let mut first = None;
    for command in commands {
        let name = command.cmd.to_ascii_uppercase();
        let sub = command
            .subcommand
            .as_ref()
            .map(|sub| sub.to_ascii_uppercase());
        for key in keys_of(&name, sub.as_deref(), &command.args) {
            let slot = redis_keyslot(&key);
            if *first.get_or_insert(slot) != slot {
                return Err(server_error(
                    "EXECABORT Transaction discarded because of previous errors: CROSSSLOT Keys \
                     in request don't hash to the same slot",
                ));
            }
        }
    }
    Ok(())
}
