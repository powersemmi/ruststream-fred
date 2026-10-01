//! Lists on the in-process server: what a list subscription pops, claims and puts back.

use std::collections::VecDeque;

use bytes::Bytes;
use fred::error::Error;
use fred::types::Value;
use tokio::time::Instant;

use super::args::{Args, bulk, int, not_integer, ok, syntax};
use super::keyspace::Write;
use super::{Server, State};

/// Which end of a list.
#[derive(Clone, Copy)]
enum End {
    Left,
    Right,
}

impl End {
    fn parse(args: &mut Args<'_>) -> Result<Self, Error> {
        if args.word("LEFT") {
            Ok(Self::Left)
        } else if args.word("RIGHT") {
            Ok(Self::Right)
        } else {
            Err(syntax())
        }
    }

    fn pop(self, list: &mut VecDeque<Bytes>) -> Option<Bytes> {
        match self {
            Self::Left => list.pop_front(),
            Self::Right => list.pop_back(),
        }
    }

    fn push(self, list: &mut VecDeque<Bytes>, value: Bytes) {
        match self {
            Self::Left => list.push_front(value),
            Self::Right => list.push_back(value),
        }
    }
}

impl State {
    /// The list commands that push and pop.
    #[allow(
        clippy::unnecessary_wraps,
        reason = "every family answers the same way"
    )]
    pub(super) fn lists(
        &mut self,
        server: &Server,
        name: &str,
        args: &mut Args<'_>,
        now: Instant,
    ) -> Result<Option<Value>, Error> {
        let _ = (server, now);
        let reply = match name {
            "LPUSH" | "RPUSH" => {
                let key = args.bytes()?;
                let values = args.rest()?;
                if values.is_empty() {
                    return Err(args.arity());
                }
                let end = if name == "LPUSH" {
                    End::Left
                } else {
                    End::Right
                };
                let list = self.keys.list_mut(&key, now, true)?.expect("created");
                for value in &values {
                    end.push(list, value.clone());
                }
                let len = list.len();
                for value in values.iter().cloned() {
                    self.log(&key, Write::Body(value));
                }
                self.list_changed(server, &key, values.len(), 0);
                Ok(int(len))
            }
            "LPOP" | "RPOP" => {
                let key = args.bytes()?;
                let count = match args.left() {
                    0 => None,
                    _ => Some(usize::try_from(args.int()?).map_err(|_| not_integer())?),
                };
                let end = if name == "LPOP" {
                    End::Left
                } else {
                    End::Right
                };
                let Some(list) = self.keys.list_mut(&key, now, false)? else {
                    return Ok(Some(Value::Null));
                };
                let popped: Vec<Bytes> = std::iter::from_fn(|| end.pop(list))
                    .take(count.unwrap_or(1))
                    .collect();
                self.keys.drop_empty(&key);
                self.list_changed(server, &key, 0, popped.len());
                Ok(match count {
                    None => popped.into_iter().next().map_or(Value::Null, bulk),
                    Some(_) if popped.is_empty() => Value::Null,
                    Some(_) => Value::Array(popped.into_iter().map(bulk).collect()),
                })
            }
            "BRPOP" | "BLPOP" => {
                let mut keys = args.rest()?;
                if keys.len() < 2 {
                    return Err(args.arity());
                }
                // The timeout: a read here never blocks, the subscriber has waited already.
                keys.pop();
                let end = if name == "BLPOP" {
                    End::Left
                } else {
                    End::Right
                };
                for key in keys {
                    if let Some(list) = self.keys.list_mut(&key, now, false)?
                        && let Some(value) = end.pop(list)
                    {
                        self.keys.drop_empty(&key);
                        self.list_changed(server, &key, 0, 1);
                        return Ok(Some(Value::Array(vec![bulk(key), bulk(value)])));
                    }
                }
                Ok(Value::Null)
            }
            "LMOVE" | "BLMOVE" | "RPOPLPUSH" | "BRPOPLPUSH" => {
                let source = args.bytes()?;
                let destination = args.bytes()?;
                let (from, to) = if name.ends_with("LMOVE") {
                    (End::parse(args)?, End::parse(args)?)
                } else {
                    (End::Right, End::Left)
                };
                if name.starts_with('B') {
                    // The timeout, which a read here never waits out.
                    args.float()?;
                }
                args.done()?;
                self.lmove(server, &source, &destination, from, to, now)
            }
            _ => return Ok(None),
        };
        reply.map(Some)
    }

    /// The list commands that read or trim a list in place.
    #[allow(
        clippy::unnecessary_wraps,
        reason = "every family answers the same way"
    )]
    pub(super) fn list_reads(
        &mut self,
        server: &Server,
        name: &str,
        args: &mut Args<'_>,
        now: Instant,
    ) -> Result<Option<Value>, Error> {
        let _ = (server, now);
        let reply = match name {
            "LLEN" => {
                let key = args.bytes()?;
                Ok(int(self.keys.list(&key, now)?.map_or(0, VecDeque::len)))
            }
            "LRANGE" => {
                let key = args.bytes()?;
                let (start, stop) = (args.int()?, args.int()?);
                let list = self.keys.list(&key, now)?;
                let len = list.map_or(0, VecDeque::len);
                Ok(Value::Array(match (list, range(start, stop, len)) {
                    (Some(list), Some((from, to))) => {
                        list.range(from..=to).cloned().map(bulk).collect()
                    }
                    _ => Vec::new(),
                }))
            }
            "LINDEX" => {
                let key = args.bytes()?;
                let index = args.int()?;
                let list = self.keys.list(&key, now)?;
                let len = list.map_or(0, VecDeque::len);
                Ok(list
                    .zip(range(index, index, len))
                    .and_then(|(list, (at, _))| list.get(at).cloned())
                    .map_or(Value::Null, bulk))
            }
            "LREM" => {
                let key = args.bytes()?;
                let count = args.int()?;
                let value = args.bytes()?;
                args.done()?;
                let removed = self
                    .keys
                    .list_mut(&key, now, false)?
                    .map_or(0, |list| remove_values(list, count, &value));
                self.keys.drop_empty(&key);
                self.recount_list(server, &key);
                Ok(int(removed))
            }
            "LTRIM" => {
                let key = args.bytes()?;
                let (start, stop) = (args.int()?, args.int()?);
                if let Some(list) = self.keys.list_mut(&key, now, false)? {
                    match range(start, stop, list.len()) {
                        Some((from, to)) => {
                            list.truncate(to + 1);
                            list.drain(..from);
                        }
                        None => list.clear(),
                    }
                }
                self.keys.drop_empty(&key);
                self.recount_list(server, &key);
                Ok(ok())
            }
            _ => return Ok(None),
        };
        reply.map(Some)
    }

    fn lmove(
        &mut self,
        server: &Server,
        source: &Bytes,
        destination: &Bytes,
        from: End,
        to: End,
        now: Instant,
    ) -> Result<Value, Error> {
        // Both types are checked before anything moves, as the server checks them.
        self.keys.list(destination, now)?;
        let Some(list) = self.keys.list_mut(source, now, false)? else {
            return Ok(Value::Null);
        };
        let Some(value) = from.pop(list) else {
            return Ok(Value::Null);
        };
        self.keys.drop_empty(source);
        let target = self
            .keys
            .list_mut(destination, now, true)?
            .expect("created");
        to.push(target, value.clone());
        self.list_changed(server, source, 0, 1);
        self.list_changed(server, destination, 1, 0);
        Ok(bulk(value))
    }
}

/// The inclusive index range `start..=stop` names on a list of `len`, negative indices counting
/// from the end, or `None` when it is empty.
fn range(start: i64, stop: i64, len: usize) -> Option<(usize, usize)> {
    let len = i64::try_from(len).ok()?;
    let start = if start < 0 {
        (len + start).max(0)
    } else {
        start
    };
    let stop = if stop < 0 {
        len + stop
    } else {
        stop.min(len - 1)
    };
    if start > stop || start >= len || stop < 0 {
        return None;
    }
    Some((usize::try_from(start).ok()?, usize::try_from(stop).ok()?))
}

/// `LREM`: removes up to `count` occurrences of `value`, from the head for a positive count, from
/// the tail for a negative one, and all of them for zero.
fn remove_values(list: &mut VecDeque<Bytes>, count: i64, value: &Bytes) -> usize {
    let limit = if count == 0 {
        usize::MAX
    } else {
        usize::try_from(count.unsigned_abs()).unwrap_or(usize::MAX)
    };
    let mut removed = 0;
    if count >= 0 {
        let mut at = 0;
        while at < list.len() && removed < limit {
            if list[at] == *value {
                list.remove(at);
                removed += 1;
            } else {
                at += 1;
            }
        }
    } else {
        let mut at = list.len();
        while at > 0 && removed < limit {
            at -= 1;
            if list[at] == *value {
                list.remove(at);
                removed += 1;
            }
        }
    }
    removed
}
