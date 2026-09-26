//! The in-process mode: a Redis server modelled inside the test process, which
//! [`RedisBroker`](crate::RedisBroker) connects to under the `testing` feature when the test
//! harness runs the app in process.
//!
//! The model sits under `fred` itself. The connected broker is the production one, its pool is
//! `fred`'s own pool over `fred`'s mock layer, and every command the crate sends (a publish, a
//! group read, an acknowledgement, a delay-queue sweep, a handler's own command) reaches the
//! model as the command it is and is answered as a server answers it. What the mock layer cannot
//! carry is waited on here instead: a blocking read waits until the model has something for it,
//! and a Pub/Sub message reaches its subscription through the channel the client's own message
//! stream would deliver it on.
//!
//! The model keeps Redis's semantics for what the crate relies on: key types and `WRONGTYPE`,
//! consumer groups with a cursor, a pending entries list and delivery counts, `XAUTOCLAIM` and
//! `XREADGROUP ... CLAIM` at an idle threshold, lists popped by one consumer, sorted sets, key
//! expiry, `PUBLISH` to every subscription of the channel and every `PSUBSCRIBE` glob that
//! matches it, `MULTI` / `EXEC`, and on a cluster topology the refusal of a command or a
//! transaction that spans hash slots. It runs no scripts: `EVAL`, `EVALSHA` and `FCALL` are
//! refused, and so is a command it does not know.
//!
//! Time is the test's clock: entry ids, idle times and the scores the delay queue and the list
//! recovery write are read off it, so a paused clock holds them and
//! [`TestApp::advance`](ruststream::testing::TestApp::advance) moves them.

mod args;
mod connection;
mod glob;
mod keyspace;
mod lists;
mod streams;

pub(crate) use glob::matches as glob_matches;

use std::collections::{BTreeSet, HashMap};
use std::fmt::{Debug, Formatter};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use fred::clients::{Client, Pool};
use fred::error::Error;
use fred::interfaces::ClientLike;
use fred::mocks::MockCommand;
use fred::types::config::{Config, PerformanceConfig, Server as ServerAddress};
use fred::types::{Message, MessageKind, Value, Version};
use ruststream::testing::Coordinator;
use tokio::runtime::Handle;
use tokio::sync::{Notify, broadcast};
use tokio::time::Instant;

use crate::error::RedisError;

pub(crate) use keyspace::Write;
use keyspace::{Keyspace, Link, LinkKind};
use streams::Id;

/// The server version the model answers as: the release whose semantics it keeps, `XREADGROUP
/// ... CLAIM` included.
pub(crate) const VERSION: Version = Version::new(8, 4, 0);

/// One subscription registered with the model.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ReaderId(u64);

/// What a subscription reads, as the model tracks it.
#[derive(Debug)]
enum Reader {
    /// A consumer of a group: `fresh` when it reads new entries, `claim` the idle threshold it
    /// claims pending entries at, `delay` the delay queue its sweeps move back onto the stream.
    Stream {
        key: Bytes,
        group: Bytes,
        consumer: Bytes,
        fresh: bool,
        claim: Option<Duration>,
        delay: Option<Bytes>,
    },
    /// A list consumer, with the recovery set its sweeps move stranded entries back from.
    List { key: Bytes, recovery: Option<Bytes> },
    /// A Pub/Sub subscription.
    Channel {
        target: Subscription,
        tx: broadcast::Sender<Message>,
    },
}

/// What a Pub/Sub subscription listens to.
#[derive(Clone, Debug)]
pub(crate) enum Subscription {
    /// `SUBSCRIBE`: one channel.
    Channel(Bytes),
    /// `SSUBSCRIBE`: one shard channel, which only `SPUBLISH` reaches.
    Sharded(Bytes),
    /// `PSUBSCRIBE`: every channel the glob matches.
    Pattern(Bytes),
}

/// The Redis server of one in-process connection.
pub(crate) struct Server {
    state: Mutex<State>,
    /// Woken on every change, so a waiting read looks again.
    changed: Notify,
    coordinator: OnceLock<Coordinator>,
    /// Whether the topology is a cluster, which is where a command spanning hash slots is refused.
    clustered: bool,
    /// The wall clock when the server started, in epoch milliseconds; the test clock moves it on.
    epoch_ms: u64,
    started: Instant,
    closed: AtomicBool,
    /// The capacity of a subscription's message channel, the client's own setting.
    capacity: usize,
    /// The address a Pub/Sub message reports it came from.
    address: ServerAddress,
    next_reader: AtomicU64,
    /// The runtime the connection was opened on, where a plain timer runs whichever caller armed
    /// it, as the broker runs its own tasks.
    runtime: Handle,
    this: Weak<Self>,
}

/// The keyspace and what the model tracks beside it.
#[derive(Default)]
pub(crate) struct State {
    keys: Keyspace,
    readers: HashMap<ReaderId, Reader>,
    /// Per stream and group, the entries counted in flight with the harness and not yet handed
    /// out.
    stream_owed: HashMap<(Bytes, Bytes), BTreeSet<Id>>,
    /// Per stream and group, the pending entries a claiming reader may take now.
    claim_due: HashMap<(Bytes, Bytes), BTreeSet<Id>>,
    /// Per list, how many of its elements are counted in flight.
    list_owed: HashMap<Bytes, usize>,
    /// Per stream, the delayed entries a sweep took off their queue and is about to add back.
    readd_holds: HashMap<Bytes, usize>,
    /// The delay queues and recovery sets a subscription sweeps, by key.
    links: HashMap<Bytes, Link>,
    /// Every write to a name, for the harness's `published`.
    log: HashMap<String, Vec<Write>>,
}

impl Debug for Server {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InProcessServer")
            .field("clustered", &self.clustered)
            .field("closed", &self.closed.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

/// Connects `pool_size` connections of `config` to a new in-process server: each connection is a
/// `fred` client over the mock layer, with a transaction state of its own. The config handed back
/// is `config` over the server, for the dedicated clients Pub/Sub subscriptions open.
///
/// # Errors
///
/// Returns [`RedisError::Connect`] when `fred` refuses the pool, as it would refuse it on a real
/// connection.
pub(crate) async fn connect(
    config: &Config,
    pool_size: usize,
    clustered: bool,
) -> Result<(Pool, Config, Arc<Server>), RedisError> {
    let address = config
        .server
        .hosts()
        .first()
        .cloned()
        .unwrap_or_else(|| ServerAddress::new("localhost", 6379));
    let capacity = config_capacity();
    let server = Arc::new_cyclic(|this| Server {
        state: Mutex::new(State::default()),
        changed: Notify::new(),
        coordinator: OnceLock::new(),
        clustered,
        epoch_ms: system_ms(),
        started: Instant::now(),
        closed: AtomicBool::new(false),
        capacity,
        address,
        next_reader: AtomicU64::new(0),
        runtime: Handle::current(),
        this: this.clone(),
    });
    // `fred` refuses an empty pool before anything is built; asking it keeps that answer.
    Pool::new(config.clone(), None, None, None, pool_size)
        .map_err(|err| RedisError::Connect(Box::new(err)))?;
    let mut clients = Vec::with_capacity(pool_size);
    for _ in 0..pool_size {
        let client = Client::new(connection::config(config, &server), None, None, None);
        client
            .init()
            .await
            .map_err(|err| RedisError::Connect(Box::new(err)))?;
        clients.push(client);
    }
    let pool = Pool::from_clients(clients).map_err(|err| RedisError::Connect(Box::new(err)))?;
    Ok((pool, connection::config(config, &server), server))
}

/// The capacity of a client's message channel: the connection builds its clients with `fred`'s
/// default performance settings, so a subscription here gets the same room before it lags.
fn config_capacity() -> usize {
    PerformanceConfig::default().broadcast_channel_capacity
}

fn system_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

impl Server {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().expect("in-process redis state poisoned")
    }

    /// The harness coordinator, once a test run installed it.
    pub(crate) fn coordinator(&self) -> Option<Coordinator> {
        self.coordinator.get().cloned()
    }

    /// Installs the harness coordinator. A second install is ignored.
    pub(crate) fn install(&self, coordinator: Coordinator) {
        let _ = self.coordinator.set(coordinator);
    }

    /// Counts `n` deliveries owed to subscriptions.
    fn owe(&self, n: usize) {
        if let Some(coordinator) = self.coordinator.get() {
            for _ in 0..n {
                coordinator.enqueued();
            }
        }
    }

    /// Releases `n` deliveries counted and never handed out.
    pub(crate) fn released(&self, n: usize) {
        if let Some(coordinator) = self.coordinator.get() {
            for _ in 0..n {
                coordinator.consumed();
            }
        }
    }

    /// The test clock.
    fn now() -> Instant {
        Instant::now()
    }

    /// The wall clock as this server keeps it, in epoch milliseconds.
    pub(crate) fn now_ms(&self) -> u64 {
        let elapsed =
            u64::try_from(Self::now().duration_since(self.started).as_millis()).unwrap_or(u64::MAX);
        self.epoch_ms.saturating_add(elapsed)
    }

    pub(crate) const fn clustered(&self) -> bool {
        self.clustered
    }

    /// Runs `fire` after `delay`: on the harness's timers when a test drives the clock, so an
    /// advance reaches it, and on a plain timer otherwise.
    fn schedule(&self, delay: Duration, fire: impl FnOnce(&Self) + Send + 'static) {
        let server = self.this.clone();
        let run = move || {
            if let Some(server) = server.upgrade() {
                fire(&server);
            }
        };
        match self.coordinator.get() {
            Some(coordinator) => coordinator.schedule_redelivery(delay, run),
            None => {
                self.runtime.spawn(async move {
                    tokio::time::sleep(delay).await;
                    run();
                });
            }
        }
    }

    /// Runs one command, as the server runs it.
    pub(crate) fn apply(&self, command: &MockCommand) -> Result<Value, Error> {
        if self.closed.load(Ordering::Acquire) {
            return Err(args::server_error("ERR the connection is closed"));
        }
        let result = {
            let mut state = self.lock();
            state.run(self, command)
        };
        self.changed.notify_waiters();
        result
    }

    /// Runs the commands of one `MULTI` / `EXEC`, all of them or, on a refusal, none.
    pub(crate) fn transaction(&self, commands: &[MockCommand]) -> Result<Value, Error> {
        if self.closed.load(Ordering::Acquire) {
            return Err(args::server_error("ERR the connection is closed"));
        }
        let result = {
            let mut state = self.lock();
            state.transaction(self, commands)
        };
        self.changed.notify_waiters();
        result
    }

    /// Marks the server closed: every later command is refused, and a waiting read returns.
    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.changed.notify_waiters();
    }

    fn next_reader(&self) -> ReaderId {
        ReaderId(self.next_reader.fetch_add(1, Ordering::Relaxed))
    }

    /// Registers a stream subscription.
    pub(crate) fn attach_stream(
        &self,
        key: &str,
        group: &str,
        consumer: &str,
        claim: Option<Duration>,
        fresh: bool,
        delay: Option<&str>,
    ) -> ReaderId {
        let id = self.next_reader();
        let key = Bytes::copy_from_slice(key.as_bytes());
        let group = Bytes::copy_from_slice(group.as_bytes());
        let delay = delay.map(|zset| Bytes::copy_from_slice(zset.as_bytes()));
        {
            let mut state = self.lock();
            if let Some(zset) = &delay {
                state
                    .links
                    .entry(zset.clone())
                    .or_insert_with(|| Link::new(key.clone(), LinkKind::Delay));
            }
            state.readers.insert(
                id,
                Reader::Stream {
                    key: key.clone(),
                    group: group.clone(),
                    consumer: Bytes::copy_from_slice(consumer.as_bytes()),
                    fresh,
                    claim,
                    delay,
                },
            );
            if let Some(min_idle) = claim {
                state.arm_claims(self, &key, &group, min_idle);
            }
            state.recount_group(self, &key, &group);
        }
        self.changed.notify_waiters();
        id
    }

    /// Registers a list subscription, with the recovery set its sweeps read.
    pub(crate) fn attach_list(&self, key: &str, recovery: Option<(&str, Duration)>) -> ReaderId {
        let id = self.next_reader();
        let key = Bytes::copy_from_slice(key.as_bytes());
        let recovery =
            recovery.map(|(zset, min_idle)| (Bytes::copy_from_slice(zset.as_bytes()), min_idle));
        {
            let mut state = self.lock();
            if let Some((zset, min_idle)) = &recovery {
                state
                    .links
                    .entry(zset.clone())
                    .or_insert_with(|| Link::new(key.clone(), LinkKind::Recovery(*min_idle)));
            }
            state.readers.insert(
                id,
                Reader::List {
                    key: key.clone(),
                    recovery: recovery.map(|(zset, _)| zset),
                },
            );
            state.recount_list(self, &key);
        }
        self.changed.notify_waiters();
        id
    }

    /// Registers a Pub/Sub subscription and hands back the channel its messages arrive on.
    pub(crate) fn attach_pubsub(
        &self,
        target: Subscription,
    ) -> (broadcast::Receiver<Message>, ReaderId) {
        let id = self.next_reader();
        let (tx, rx) = broadcast::channel(self.capacity);
        self.lock()
            .readers
            .insert(id, Reader::Channel { target, tx });
        (rx, id)
    }

    /// Removes a subscription, releasing what was counted for it alone.
    pub(crate) fn detach(&self, reader: ReaderId) {
        {
            let mut state = self.lock();
            match state.readers.remove(&reader) {
                Some(Reader::Stream { key, group, .. }) => state.recount_group(self, &key, &group),
                Some(Reader::List { key, .. }) => state.recount_list(self, &key),
                Some(Reader::Channel { .. }) | None => {}
            }
        }
        self.changed.notify_waiters();
    }

    /// Waits until a read of `reader` would find something, or the server closed.
    pub(crate) async fn readable(&self, reader: ReaderId) {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.closed.load(Ordering::Acquire) || self.lock().readable(self, reader) {
                return;
            }
            notified.await;
        }
    }

    /// Runs one write the harness injects, as the command an external producer sends.
    fn inject(&self, command: &str, args: Vec<Value>) -> Result<Value, Error> {
        self.apply(&MockCommand {
            cmd: command.into(),
            subcommand: None,
            args,
        })
    }

    /// `XADD name * fields..`, for the harness.
    pub(crate) fn inject_stream(
        &self,
        name: &str,
        fields: Vec<(String, Vec<u8>)>,
    ) -> Result<(), Error> {
        let mut args = vec![Value::from(name), Value::from("*")];
        for (field, value) in fields {
            args.push(Value::from(field));
            args.push(Value::Bytes(value.into()));
        }
        self.inject("XADD", args).map(drop)
    }

    /// `LPUSH name body`, for the harness.
    pub(crate) fn inject_list(&self, name: &str, body: Vec<u8>) -> Result<(), Error> {
        self.inject("LPUSH", vec![Value::from(name), Value::Bytes(body.into())])
            .map(drop)
    }

    /// `PUBLISH name body`, or `SPUBLISH` when `sharded`, for the harness.
    pub(crate) fn inject_publish(
        &self,
        name: &str,
        body: Vec<u8>,
        sharded: bool,
    ) -> Result<(), Error> {
        let command = if sharded { "SPUBLISH" } else { "PUBLISH" };
        self.inject(command, vec![Value::from(name), Value::Bytes(body.into())])
            .map(drop)
    }

    /// Every write to `name` so far, in order.
    pub(crate) fn published(&self, name: &str) -> Vec<Write> {
        self.lock().log.get(name).cloned().unwrap_or_default()
    }

    /// A Pub/Sub message as the client's message stream carries it.
    fn message(&self, channel: &Bytes, body: &Bytes, kind: MessageKind) -> Message {
        Message {
            channel: String::from_utf8_lossy(channel).as_ref().into(),
            value: Value::Bytes(body.clone()),
            kind,
            server: self.address.clone(),
        }
    }
}

impl State {
    /// Whether a read of `reader` finds something now.
    fn readable(&self, server: &Server, reader: ReaderId) -> bool {
        let now = Server::now();
        match self.readers.get(&reader) {
            Some(Reader::Stream {
                key,
                group,
                fresh,
                claim,
                delay,
                ..
            }) => {
                let due = |links: &HashMap<Bytes, Link>, zset: &Option<Bytes>| {
                    zset.as_ref()
                        .and_then(|zset| links.get(zset))
                        .is_some_and(|link| !link.due.is_empty())
                };
                if due(&self.links, delay) {
                    return true;
                }
                if claim.is_some()
                    && self
                        .claim_due
                        .get(&(key.clone(), group.clone()))
                        .is_some_and(|due| !due.is_empty())
                {
                    return true;
                }
                // A read of a key of another type fails at once on a server, so it is not waited.
                self.keys.stream(key, now).is_err()
                    || (*fresh && self.keys.has_new_entries(key, group, now))
            }
            Some(Reader::List { key, recovery }) => {
                recovery
                    .as_ref()
                    .and_then(|zset| self.links.get(zset))
                    .is_some_and(|link| !link.due.is_empty())
                    || self.keys.list(key, now).is_err()
                    || self.keys.list_len(key, now) > 0
            }
            Some(Reader::Channel { .. }) | None => {
                let _ = server;
                false
            }
        }
    }

    fn run(&mut self, server: &Server, command: &MockCommand) -> Result<Value, Error> {
        let name = command.cmd.to_ascii_uppercase();
        let sub = command
            .subcommand
            .as_ref()
            .map(|sub| sub.to_ascii_uppercase());
        if server.clustered() {
            keyspace::check_one_slot(&name, sub.as_deref(), &command.args)?;
        }
        self.dispatch(server, &name, sub.as_deref(), &command.args)
    }

    fn transaction(&mut self, server: &Server, commands: &[MockCommand]) -> Result<Value, Error> {
        if server.clustered() {
            keyspace::check_transaction_slot(commands)?;
        }
        let mut replies = Vec::with_capacity(commands.len());
        let mut first_error = None;
        for command in commands {
            match self.run(server, command) {
                Ok(reply) => replies.push(reply),
                // A command that fails inside `EXEC` does not undo the others, as on the server;
                // the caller hears the first failure.
                Err(err) => {
                    first_error.get_or_insert(err);
                    replies.push(Value::Null);
                }
            }
        }
        first_error.map_or(Ok(Value::Array(replies)), Err)
    }

    /// Whether a registered reader of `key` reads through `group`, and how.
    fn group_readers(&self, key: &Bytes, group: &Bytes) -> GroupReaders {
        let mut readers = GroupReaders::default();
        for reader in self.readers.values() {
            if let Reader::Stream {
                key: read,
                group: through,
                fresh,
                claim,
                ..
            } = reader
                && read == key
                && through == group
            {
                readers.any = true;
                readers.fresh |= *fresh;
                readers.claim = match (readers.claim, *claim) {
                    (Some(held), Some(new)) => Some(held.min(new)),
                    (held, new) => held.or(new),
                };
            }
        }
        readers
    }

    /// Whether `consumer` is a registered subscription of `key` through `group`.
    fn registered(&self, key: &Bytes, group: &Bytes, consumer: &Bytes) -> bool {
        self.readers.values().any(|reader| {
            matches!(reader, Reader::Stream { key: read, group: through, consumer: name, .. }
                if read == key && through == group && name == consumer)
        })
    }

    fn list_read(&self, key: &Bytes) -> bool {
        self.readers
            .values()
            .any(|reader| matches!(reader, Reader::List { key: read, .. } if read == key))
    }

    /// Brings the count of a group's owed entries to what its readers are owed now.
    fn recount_group(&mut self, server: &Server, key: &Bytes, group: &Bytes) {
        let readers = self.group_readers(key, group);
        let slot = (key.clone(), group.clone());
        let mut desired = BTreeSet::new();
        if readers.any {
            if readers.fresh {
                desired.extend(self.keys.new_entries(key, group, Server::now()));
            }
            if readers.claim.is_some()
                && let Some(due) = self.claim_due.get(&slot)
            {
                desired.extend(due.iter().copied());
            }
        }
        let owed = self.stream_owed.entry(slot).or_default();
        let extra = owed.difference(&desired).count();
        let missing = desired.difference(owed).count();
        *owed = desired;
        server.released(extra);
        server.owe(missing);
    }

    /// Brings the count of a list's owed elements to its length, or to nothing without a reader.
    fn recount_list(&mut self, server: &Server, key: &Bytes) {
        let desired = if self.list_read(key) {
            self.keys.list_len(key, Server::now())
        } else {
            0
        };
        let owed = self.list_owed.entry(key.clone()).or_default();
        if *owed > desired {
            server.released(*owed - desired);
        } else {
            server.owe(desired - *owed);
        }
        *owed = desired;
    }

    /// Counts what a change to a list owes: `pushed` new elements, `popped` taken off it.
    fn list_changed(&mut self, server: &Server, key: &Bytes, pushed: usize, popped: usize) {
        if !self.list_read(key) {
            return;
        }
        let owed = self.list_owed.entry(key.clone()).or_default();
        *owed += pushed;
        server.owe(pushed);
        // A popped element's count passes to the delivery that carries it; one never counted is
        // counted now, so that delivery's release balances it.
        let transferred = popped.min(*owed);
        *owed -= transferred;
        server.owe(popped - transferred);
    }

    /// Records a write to `name` for the harness.
    fn log(&mut self, name: &Bytes, write: Write) {
        self.log
            .entry(String::from_utf8_lossy(name).into_owned())
            .or_default()
            .push(write);
    }

    /// Delivers a Pub/Sub message to every subscription it reaches, and answers how many.
    fn publish(&mut self, server: &Server, channel: &Bytes, body: &Bytes, sharded: bool) -> usize {
        self.log(channel, Write::Body(body.clone()));
        let mut reached = 0;
        for reader in self.readers.values() {
            let Reader::Channel { target, tx } = reader else {
                continue;
            };
            let kind = match (target, sharded) {
                (Subscription::Channel(name), false) if name == channel => MessageKind::Message,
                (Subscription::Sharded(name), true) if name == channel => MessageKind::SMessage,
                (Subscription::Pattern(glob), false) if glob::matches(glob, channel) => {
                    MessageKind::PMessage
                }
                _ => continue,
            };
            if tx.send(server.message(channel, body, kind)).is_ok() {
                reached += 1;
                server.owe(1);
            }
        }
        reached
    }

    /// Arms the wait of a delay-queue or recovery member written at `score`.
    fn arm_link(&self, server: &Server, zset: &Bytes, member: &Bytes, score: f64) {
        let Some(link) = self.links.get(zset) else {
            return;
        };
        let due_ms = match link.kind {
            LinkKind::Delay => score,
            #[allow(
                clippy::cast_precision_loss,
                reason = "an idle threshold is far below 2^53 ms"
            )]
            LinkKind::Recovery(min_idle) => score + min_idle.as_millis() as f64,
        };
        #[allow(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            reason = "a due time in epoch milliseconds, clamped at zero"
        )]
        let due_ms = due_ms.max(0.0) as u64;
        let wait = Duration::from_millis(due_ms.saturating_sub(server.now_ms()));
        let (zset, member) = (zset.clone(), member.clone());
        server.schedule(wait, move |server| {
            {
                let mut state = server.lock();
                state.link_due(server, &zset, &member, score);
            }
            server.changed.notify_waiters();
        });
    }

    /// Marks a delay-queue or recovery member due, when it is still there at the score it was
    /// armed with, and counts the delivery its sweep owes.
    fn link_due(&mut self, server: &Server, zset: &Bytes, member: &Bytes, score: f64) {
        #[allow(
            clippy::float_cmp,
            reason = "the very score the member was written with"
        )]
        let current = self.keys.zscore(zset, member, Server::now()) == Some(score);
        if !current {
            return;
        }
        let Some(target) = self.links.get(zset).map(|link| link.target.clone()) else {
            return;
        };
        let read = self.list_read(&target)
            || self.readers.values().any(
                |reader| matches!(reader, Reader::Stream { key, fresh: true, .. } if *key == target),
            );
        let Some(link) = self.links.get_mut(zset) else {
            return;
        };
        link.due.insert(member.clone());
        if read && server.coordinator.get().is_some() && link.held.insert(member.clone()) {
            server.owe(1);
        }
    }

    /// Forgets a member leaving a linked set; a counted one passes its count on to what the
    /// sweep adds back.
    fn link_removed(&mut self, server: &Server, zset: &Bytes, member: &Bytes) {
        let Some(link) = self.links.get_mut(zset) else {
            return;
        };
        link.due.remove(member);
        if link.held.remove(member) {
            match link.kind {
                // The sweep removes the member first and adds the entry back after: the count
                // waits for that `XADD`.
                LinkKind::Delay => *self.readd_holds.entry(link.target.clone()).or_default() += 1,
                // The recovery sweep pushed the entry back before it removed the member.
                LinkKind::Recovery(_) => server.released(1),
            }
        }
    }

    /// Forgets a linked set's members when the set itself goes: no sweep is left to move them
    /// back, so the counts held for its due members are released at once.
    fn link_dropped(&mut self, server: &Server, zset: &Bytes) {
        let Some(link) = self.links.get_mut(zset) else {
            return;
        };
        link.due.clear();
        let released = std::mem::take(&mut link.held).len();
        if released > 0 {
            server.released(released);
        }
    }

    /// Releases the count of a delayed entry its sweep just added back to `key`.
    fn added_back(&mut self, server: &Server, key: &Bytes) {
        if let Some(held) = self.readd_holds.get_mut(key)
            && *held > 0
        {
            *held -= 1;
            server.released(1);
        }
    }
}

/// How the registered readers of one group read.
#[derive(Default, Debug, Clone, Copy)]
struct GroupReaders {
    any: bool,
    fresh: bool,
    claim: Option<Duration>,
}
