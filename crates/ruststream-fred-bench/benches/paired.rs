// The benchmark is a binary of its own, not library surface: a measured loop panics on a broker
// fault rather than threading a `Result` through a scenario nobody recovers from.
#![allow(
    missing_docs,
    unreachable_pub,
    unused_qualifications,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
//! What this crate costs over the `fred` client it wraps, and what the runtime costs on top.
//!
//! One scenario, run three ways. **raw** drives `fred` directly, in a loop written by hand.
//! **adapter** drives this crate in the same kind of loop - the broker, the subscription, the
//! [`Subscriber`] stream it yields, the delivery and its `ack` - with no service, no handler and
//! no dispatch. **framework** is the service a user writes, started through the real runtime.
//!
//! Two differences come out of that. `adapter` against `raw` is what this crate's own consumer
//! costs over the client it wraps, which is the number this repository is responsible for.
//! `framework` against `adapter` is what the runtime costs on top of this broker in particular:
//! every adapter is thin, so a share that differs between brokers lives in how the two meet - how
//! the stream yields, how deliveries arrive, how back-pressure reaches the consumer - and that is
//! a finding about this crate.
//!
//! The three loops differ in that and in nothing else - same pool and pool size, same consumer
//! group and consumer name, same read commands with the same `COUNT` and `BLOCK`, same ack
//! position, same decode into the same type, the same payload bytes, the same tokio runtime and
//! the same binary. The procedure the numbers follow is the framework's own, published at
//! <https://powersemmi.github.io/ruststream/latest/benchmarks/>.
//!
//! # What a run is
//!
//! The consumer is attached first, a pipelined publisher on a connection of its own then feeds it,
//! and the window runs from the first delivery to the last. Connecting, creating the consumer
//! group and opening the subscription are startup cost and sit outside it. Every run gets a fresh
//! key, a fresh consumer group and a fresh processing list, so a run never sees what the one
//! before it left behind, and it unlinks them when it is done.
//!
//! The publisher is the same on all three loops and is a fixture rather than a subject: it is raw
//! `fred`, pipelined, so the differences between the loops stay on the consuming side. What this
//! crate's own publisher costs is a measurement of its own and is not this one.
//!
//! The message count is not a constant: a probe run measures the raw loop's rate and the count is
//! set from it, so a measured run lasts at least [`SECONDS`] on whatever machine it is taken on.
//!
//! Rounds are interleaved - raw, adapter, framework, and again - and the first is discarded.
//! Running one loop to the end and then the next would charge every drift of the machine to
//! whichever ran last.
//!
//! # What the numbers do not say
//!
//! The window ends where the delivery is taken rather than after its acknowledgement, on all three
//! loops alike, because that is the point every loop can observe. One acknowledgement out of
//! hundreds of thousands is far below the run-to-run spread.
//!
//! The run turns the server's append-only file off. What is measured is the cost of a delivery,
//! not the disk under the server, and an `fsync` that lands inside one loop of a round is noise
//! that belongs to none of them. The stand is the one the tests use and the recipe tears it down
//! afterwards, so the setting outlives nothing.
//!
//! A consumer that spends its window waiting on the socket was paced by the server, and the row is
//! reported as broker-bound: what it measures then is the machine's loopback and the server, not
//! this crate.

use std::convert::Infallible;
use std::env;
use std::fmt::Write as _;
use std::hint::black_box;
use std::pin::pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use fred::clients::{Client, Pool};
use fred::error::{Error as FredError, ErrorKind};
use fred::interfaces::{
    ClientLike, ConfigInterface, EventInterface, KeysInterface, ListInterface, PubsubInterface,
    StreamsInterface,
};
use fred::types::Value;
use fred::types::config::Config;
use fred::types::lists::LMoveDirection;
use futures::StreamExt;
use ruststream::runtime::RunningApp;
use ruststream::{ConnectedBroker, Subscriber};
use ruststream_fred::ConnectedRedisBroker;
use ruststream_fred::prelude::*;
use serde::Deserialize;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::Notify;
use tokio::sync::broadcast::error::RecvError;
use tokio::time::{sleep, timeout};

// A benchmark measures what ships. The framework's harness feature swaps the broker for an
// in-process stand-in, so a number taken with it on is not this crate's transport at all. The
// benchmark lives in a package of its own for the same reason: `ruststream-fred`'s
// dev-dependencies enable that feature through the conformance harness, and a benchmark inside
// that package would link it.
#[cfg(feature = "testing")]
compile_error!(
    "benchmarks must be built without the `testing` feature; run them through `just bench`"
);

/// Deliveries the probe run takes to measure the raw loop's rate.
const PROBE_MESSAGES: usize = 50_000;
/// How long a measured run lasts, at least.
const SECONDS: f64 = 5.0;
/// How much the calibrated count is raised above the probe's estimate.
///
/// The probe is short and cold, so it reads the machine low; without the margin the fastest
/// scenario lands just under the floor.
const MARGIN: f64 = 1.25;
/// The ceiling on a calibrated count.
///
/// A stream keeps every entry it was given, acknowledged or not, so a run's key holds the whole
/// count until it is unlinked. This bounds what one run asks the server to hold.
const MAX_MESSAGES: usize = 2_000_000;
/// Rounds kept. One more is run and discarded.
const ROUNDS: usize = 11;
/// Worker threads both halves are driven on.
const WORKERS: usize = 4;
/// Connections in the pool both halves read and settle through. The broker's own default.
const POOL: usize = 4;

/// `COUNT` on a stream read, and this crate's own value for a subscription that names no batch
/// size. A read that fetched one entry per round trip would spend a round trip per message.
const PREFETCH: u64 = 64;
/// `BLOCK` on a stream read, and the seconds a list pop blocks for. This crate's defaults.
const BLOCK_MS: u64 = 5_000;
const BLOCK_SECS: f64 = 5.0;

/// The field a stream entry carries its body in, and the field this crate reads it from.
const PAYLOAD_FIELD: &str = "_payload";

/// How far the publisher may run ahead of the consumer, in messages.
///
/// The queue between the two halves is the server's, so this is what a run asks Redis to hold at
/// once. Large enough that the consumer is never waiting for a body, small enough that a run's
/// backlog stays a few megabytes.
const IN_FLIGHT: usize = 8_192;
/// Commands per pipelined publish. The publisher is not the subject: without pipelining it would
/// spend a round trip per message and pace the consumer instead of feeding it.
const BATCH: usize = 256;
/// How long a run may go without a delivery before it is called stuck.
const STALL: Duration = Duration::from_secs(30);
/// How long the publisher parks before it looks at the consumer again.
const STEP: Duration = Duration::from_micros(200);

/// Commands the round-trip probe sends. What it measures decides the `broker_bound` mark: a
/// scenario whose deliveries cost more round trips than they have time is one the server paced.
const ROUND_TRIP_PROBES: usize = 20_000;

/// The body size both halves publish and decode.
const BODY_BYTES: usize = 512;
/// The values every body carries. Fixed, so every delivery of a run costs the same.
const ID: u64 = 1_000_000;
const QUANTITY: u32 = 37;

/// What both halves decode a delivery into.
///
/// Two integer fields the handler reads, and a padding the type ignores: a decode that allocates
/// nothing, so the number is about this crate rather than about `serde_json`'s string handling.
#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
    quantity: u32,
}

/// A JSON body carrying the two fields, padded with fields [`Order`] ignores until it reaches
/// roughly `size` bytes.
fn json_body(size: usize) -> Vec<u8> {
    let mut body = format!("{{\"id\":{ID},\"quantity\":{QUANTITY}");
    let mut field = 0u32;
    while body.len() + 2 < size {
        write!(body, ",\"f{field}\":\"{field:016}\"").expect("writing to a String");
        field += 1;
    }
    body.push('}');
    body.into_bytes()
}

/// The names one run owns: nothing is shared with the run before it.
#[derive(Clone, Debug)]
struct Names {
    key: String,
    group: String,
    consumer: String,
}

impl Names {
    fn fresh() -> Self {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|since| since.as_nanos())
            .unwrap_or_default();
        Self {
            key: format!("ruststream:bench:{stamp}"),
            group: format!("bench-{stamp}"),
            consumer: "bench".to_owned(),
        }
    }

    /// The processing list a reliable list subscription moves an entry to, which this crate
    /// derives from the key the same way.
    fn processing(&self) -> String {
        format!("{}.processing", self.key)
    }
}

/// The names the service being built subscribes to.
///
/// `#[subscriber(..)]` takes an expression and evaluates it where the handler is mounted, which is
/// inside the builder of the run that is starting. A run installs its own names here first, so the
/// subscription the framework opens is the one this run publishes to.
static NAMES: Mutex<Option<Names>> = Mutex::new(None);

fn install(names: &Names) {
    *NAMES
        .lock()
        .expect("the names cell is never held across a panic") = Some(names.clone());
}

fn installed() -> Names {
    NAMES
        .lock()
        .expect("the names cell is never held across a panic")
        .clone()
        .expect("a run installs its names before it builds the service")
}

/// Counts deliveries and marks the ends of the measured window.
///
/// Every loop calls the same methods, so every loop pays for the signal. A delivery pays one
/// relaxed increment and two comparisons; the waiter is a single future for the whole run,
/// woken once.
#[derive(Clone, Debug)]
struct Run(Arc<RunInner>);

#[derive(Debug)]
struct RunInner {
    total: usize,
    seen: AtomicUsize,
    first: OnceLock<Instant>,
    last: OnceLock<Instant>,
    drained: Notify,
}

impl Run {
    fn new(total: usize) -> Self {
        Self(Arc::new(RunInner {
            total,
            seen: AtomicUsize::new(0),
            first: OnceLock::new(),
            last: OnceLock::new(),
            drained: Notify::new(),
        }))
    }

    /// Records one handled delivery, and answers whether the run is over.
    fn arrived(&self) -> bool {
        let seen = self.0.seen.fetch_add(1, Ordering::Relaxed) + 1;
        if seen == 1 {
            let _ = self.0.first.set(Instant::now());
        }
        if seen == self.0.total {
            let _ = self.0.last.set(Instant::now());
            self.0.drained.notify_one();
        }
        seen >= self.0.total
    }

    fn handled(&self) -> usize {
        self.0.seen.load(Ordering::Acquire).min(self.0.total)
    }

    fn total(&self) -> usize {
        self.0.total
    }

    /// Resolves once every expected delivery has been handled.
    async fn drained(&self) {
        while self.0.seen.load(Ordering::Acquire) < self.0.total {
            self.0.drained.notified().await;
        }
    }

    /// The measured window: the first delivery to the end of the last handler call.
    fn window(&self) -> Duration {
        let first = *self.0.first.get().expect("the run took a delivery");
        let last = *self.0.last.get().expect("the run took its last delivery");
        last - first
    }
}

/// Waits for the run to finish, and fails with what it was waiting for if it stops moving.
async fn drain(run: &Run, loop_name: &str) {
    let mut seen = 0;
    loop {
        if timeout(STALL, run.drained()).await.is_ok() {
            return;
        }
        let handled = run.handled();
        assert!(
            handled > seen,
            "{loop_name}: {handled} of {} deliveries handled and nothing moved for {STALL:?}",
            run.0.total
        );
        seen = handled;
    }
}

/// What one measured run produced.
#[derive(Clone, Copy, Debug)]
struct Sample {
    window: Duration,
}

impl Sample {
    fn rate(self, messages: usize) -> f64 {
        messages as f64 / self.window.as_secs_f64()
    }
}

/// Opens the pool [`RedisBroker::standalone`] opens: the same config, the same size, and nothing
/// configured beyond it.
async fn pool(url: &str, size: usize) -> Pool {
    let config = Config::from_url(url).expect("the URL is a Redis URL");
    let pool = Pool::new(config, None, None, None, size).expect("the pool is built");
    pool.init().await.expect("the server accepts a connection");
    pool
}

async fn close(client: impl ClientLike) {
    client.quit().await.expect("the connection closes");
}

/// Removes what a run left on the server. Unlinked rather than deleted: freeing a stream of a
/// million entries on the command thread would land inside the next run's window.
async fn unlink(pool: &Pool, keys: &[&str]) {
    for key in keys {
        let _: i64 = pool.unlink(*key).await.expect("the key is unlinked");
    }
}

// ---------------------------------------------------------------------------------------------
// The publisher
// ---------------------------------------------------------------------------------------------

/// Waits until the consumer is within [`IN_FLIGHT`] of what has been sent.
///
/// A consumer that has stopped taking deliveries would otherwise park the publisher here for the
/// rest of the day, and a run that hangs says less than a run that fails.
async fn throttle(sent: usize, run: &Run) {
    let mut seen = run.handled();
    let mut waited = Duration::ZERO;
    while sent.saturating_sub(run.handled()) > IN_FLIGHT {
        sleep(STEP).await;
        waited += STEP;
        if waited >= STALL {
            let handled = run.handled();
            assert!(
                handled > seen,
                "the publisher waited {STALL:?} with {handled} of {sent} published deliveries \
                 handled"
            );
            seen = handled;
            waited = Duration::ZERO;
        }
    }
}

/// Fills the run's stream, in pipelined batches.
async fn publish_stream(pool: &Pool, key: &str, messages: usize, run: &Run) {
    let body = json_body(BODY_BYTES);
    let mut sent = 0;
    while sent < messages {
        throttle(sent, run).await;
        let batch = BATCH.min(messages - sent);
        let pipeline = pool.next().pipeline();
        for _ in 0..batch {
            let fields = vec![(PAYLOAD_FIELD.to_owned(), body.clone())];
            let _: () = pipeline
                .xadd(key, false, None::<()>, "*", fields)
                .await
                .expect("the entry is queued");
        }
        let _: Vec<Value> = pipeline
            .all()
            .await
            .expect("the server accepts the entries");
        sent += batch;
    }
}

/// Fills the run's list, in pipelined batches.
async fn publish_list(pool: &Pool, key: &str, messages: usize, run: &Run) {
    let body = json_body(BODY_BYTES);
    let mut sent = 0;
    while sent < messages {
        throttle(sent, run).await;
        let batch = BATCH.min(messages - sent);
        let pipeline = pool.next().pipeline();
        for _ in 0..batch {
            let _: () = pipeline
                .lpush(key, body.clone())
                .await
                .expect("the job is queued");
        }
        let _: Vec<Value> = pipeline.all().await.expect("the server accepts the jobs");
        sent += batch;
    }
}

/// Broadcasts on the run's channel until the consumer has handled what the run asked for.
///
/// Pub/Sub is fire-and-forget: a delivery the consumer is not there to take is dropped rather than
/// queued, so a publisher that stopped at an exact count would leave a run one delivery short of
/// its own end. Every loop is fed this way, and the rate a run reports is what the consumer
/// handled per second either way.
async fn publish_pubsub(pool: &Pool, channel: &str, run: &Run) {
    let body = json_body(BODY_BYTES);
    while run.handled() < run.total() {
        let pipeline = pool.next().pipeline();
        for _ in 0..BATCH {
            let _: () = pipeline
                .publish(channel, body.clone())
                .await
                .expect("the message is queued");
        }
        let _: Vec<Value> = pipeline
            .all()
            .await
            .expect("the server accepts the messages");
    }
}

// ---------------------------------------------------------------------------------------------
// The framework loop: the service a user writes
// ---------------------------------------------------------------------------------------------

#[subscriber(
    RedisStream::new(installed().key)
        .group(installed().group)
        .consumer(installed().consumer)
)]
async fn stream_consume(order: &Order, ctx: &mut Context<'_, (), Run>) -> HandlerOutcome {
    black_box((order.id, order.quantity));
    ctx.state().arrived();
    HandlerOutcome::ack()
}

#[subscriber(RedisList::new(installed().key).reliable())]
async fn list_consume(order: &Order, ctx: &mut Context<'_, (), Run>) -> HandlerOutcome {
    black_box((order.id, order.quantity));
    ctx.state().arrived();
    HandlerOutcome::ack()
}

#[subscriber(RedisPubSub::new(installed().key))]
async fn pubsub_consume(order: &Order, ctx: &mut Context<'_, (), Run>) -> HandlerOutcome {
    black_box((order.id, order.quantity));
    ctx.state().arrived();
    HandlerOutcome::ack()
}

async fn start_stream(url: &str, run: Run) -> RunningApp {
    RustStream::new(AppInfo::new("fred-bench", "0.0.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(run))
        .with_broker(RedisBroker::standalone(url).pool(POOL), |b| {
            b.include(stream_consume);
        })
        .start()
        .await
        .expect("the service starts")
}

async fn start_list(url: &str, run: Run) -> RunningApp {
    RustStream::new(AppInfo::new("fred-bench", "0.0.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(run))
        .with_broker(RedisBroker::standalone(url).pool(POOL), |b| {
            b.include(list_consume);
        })
        .start()
        .await
        .expect("the service starts")
}

async fn start_pubsub(url: &str, run: Run) -> RunningApp {
    RustStream::new(AppInfo::new("fred-bench", "0.0.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(run))
        .with_broker(RedisBroker::standalone(url).pool(POOL), |b| {
            b.include(pubsub_consume);
        })
        .start()
        .await
        .expect("the service starts")
}

// ---------------------------------------------------------------------------------------------
// The adapter loop: this crate's own consumer, hand-driven
// ---------------------------------------------------------------------------------------------

/// Connects the broker the way a service connects it, and nothing more: the synchronous
/// constructor, the pool size the broker defaults to, and the one consuming transition.
async fn connect(url: &str) -> ConnectedRedisBroker {
    RedisBroker::standalone(url)
        .pool(POOL)
        .connect()
        .await
        .expect("the broker connects")
}

async fn adapter_stream(url: &str, names: &Names, messages: usize) -> Sample {
    let connected = connect(url).await;
    let subscriber = connected
        .subscribe(
            RedisStream::new(names.key.clone())
                .group(names.group.clone())
                .consumer(names.consumer.clone()),
        )
        .await
        .expect("the subscription opens");

    let run = Run::new(messages);
    let consuming = tokio::spawn({
        let run = run.clone();
        async move {
            let mut subscriber = subscriber;
            let mut deliveries = pin!(subscriber.stream());
            while let Some(delivery) = deliveries.next().await {
                let delivery = delivery.expect("the subscription delivers");
                let order: Order =
                    serde_json::from_slice(delivery.payload()).expect("the body decodes");
                black_box((order.id, order.quantity));
                // The window closes on the delivery, before its acknowledgement, in every loop.
                let done = run.arrived();
                delivery
                    .ack()
                    .await
                    .expect("the acknowledgement reaches the server");
                if done {
                    break;
                }
            }
        }
    });

    let publishing = pool(url, 1).await;
    publish_stream(&publishing, &names.key, messages, &run).await;
    drain(&run, "adapter stream").await;
    consuming.await.expect("the consuming task ends");

    let sample = Sample {
        window: run.window(),
    };
    unlink(&publishing, &[&names.key]).await;
    close(publishing).await;
    connected.shutdown().await.expect("the broker shuts down");
    sample
}

async fn adapter_list(url: &str, names: &Names, messages: usize) -> Sample {
    let connected = connect(url).await;
    let subscriber = connected
        .subscribe_list(RedisList::new(names.key.clone()).reliable())
        .await
        .expect("the subscription opens");

    let run = Run::new(messages);
    let consuming = tokio::spawn({
        let run = run.clone();
        async move {
            let mut subscriber = subscriber;
            let mut deliveries = pin!(subscriber.stream());
            while let Some(delivery) = deliveries.next().await {
                let delivery = delivery.expect("the subscription delivers");
                let order: Order =
                    serde_json::from_slice(delivery.payload()).expect("the body decodes");
                black_box((order.id, order.quantity));
                let done = run.arrived();
                delivery
                    .ack()
                    .await
                    .expect("the entry leaves the processing list");
                if done {
                    break;
                }
            }
        }
    });

    let publishing = pool(url, 1).await;
    publish_list(&publishing, &names.key, messages, &run).await;
    drain(&run, "adapter list").await;
    consuming.await.expect("the consuming task ends");

    let sample = Sample {
        window: run.window(),
    };
    unlink(&publishing, &[&names.key, &names.processing()]).await;
    close(publishing).await;
    connected.shutdown().await.expect("the broker shuts down");
    sample
}

async fn adapter_pubsub(url: &str, names: &Names, messages: usize) -> Sample {
    let connected = connect(url).await;
    let subscriber = connected
        .subscribe_pubsub(RedisPubSub::new(names.key.clone()))
        .await
        .expect("the subscription opens");

    let run = Run::new(messages);
    let consuming = tokio::spawn({
        let run = run.clone();
        async move {
            let mut subscriber = subscriber;
            let mut deliveries = pin!(subscriber.stream());
            while let Some(delivery) = deliveries.next().await {
                let delivery = delivery.expect("the subscription delivers");
                let order: Order =
                    serde_json::from_slice(delivery.payload()).expect("the body decodes");
                black_box((order.id, order.quantity));
                // Pub/Sub settles nothing, so no loop acknowledges here.
                if run.arrived() {
                    break;
                }
            }
        }
    });

    let publishing = pool(url, 1).await;
    publish_pubsub(&publishing, &names.key, &run).await;
    drain(&run, "adapter pubsub").await;
    consuming.await.expect("the consuming task ends");

    let sample = Sample {
        window: run.window(),
    };
    close(publishing).await;
    connected.shutdown().await.expect("the broker shuts down");
    sample
}

// ---------------------------------------------------------------------------------------------
// The raw loop: the client, hand-driven
// ---------------------------------------------------------------------------------------------

/// `XREADGROUP` reply shape, spelled out the way this crate spells it: the RESP2 reply is an array
/// of `[key, [[id, [field, value, ...]], ...]]`, which fred's map-based reader cannot parse.
type RawStreams = Vec<(String, Vec<(String, Vec<(String, Vec<u8>)>)>)>;

/// Creates the consumer group the way this crate creates it: `MKSTREAM`, starting at the tail,
/// and an existing group is not an error.
async fn create_group(pool: &Pool, names: &Names) {
    let created: Result<String, FredError> = pool
        .xgroup_create(names.key.as_str(), names.group.as_str(), "$", true)
        .await;
    match created {
        Ok(_) => {}
        Err(err) if err.details().contains("BUSYGROUP") => {}
        Err(err) => panic!("the consumer group is created: {err}"),
    }
}

async fn raw_stream(url: &str, names: &Names, messages: usize) -> Sample {
    let consuming_pool = pool(url, POOL).await;
    create_group(&consuming_pool, names).await;

    let run = Run::new(messages);
    let consuming = tokio::spawn({
        let pool = consuming_pool.clone();
        let names = names.clone();
        let run = run.clone();
        async move {
            'reading: loop {
                let reply: RawStreams = pool
                    .xreadgroup(
                        names.group.as_str(),
                        names.consumer.as_str(),
                        Some(PREFETCH),
                        Some(BLOCK_MS),
                        false,
                        names.key.as_str(),
                        ">",
                    )
                    .await
                    .expect("the server answers the read");
                for (_, entries) in reply {
                    for (id, fields) in entries {
                        let payload = fields
                            .iter()
                            .find(|(name, _)| name == PAYLOAD_FIELD)
                            .map(|(_, value)| value.as_slice())
                            .expect("the entry carries a body");
                        let order: Order =
                            serde_json::from_slice(payload).expect("the body decodes");
                        black_box((order.id, order.quantity));
                        // The window closes on the delivery, before its acknowledgement, in
                        // every loop.
                        let done = run.arrived();
                        let _: i64 = pool
                            .xack(names.key.as_str(), names.group.as_str(), id)
                            .await
                            .expect("the acknowledgement reaches the server");
                        if done {
                            break 'reading;
                        }
                    }
                }
            }
        }
    });

    let publishing = pool(url, 1).await;
    publish_stream(&publishing, &names.key, messages, &run).await;
    drain(&run, "raw stream").await;
    consuming.await.expect("the consuming task ends");

    let sample = Sample {
        window: run.window(),
    };
    unlink(&publishing, &[&names.key]).await;
    close(publishing).await;
    close(consuming_pool).await;
    sample
}

/// Normalizes a blocking pop the way this crate does: fred reports a pop that timed out with
/// nothing available as a timeout error rather than an empty reply.
fn empty_on_timeout(result: Result<Option<Vec<u8>>, FredError>) -> Option<Vec<u8>> {
    match result {
        Ok(value) => value,
        Err(err) if matches!(err.kind(), ErrorKind::Timeout) => None,
        Err(err) => panic!("the server answers the pop: {err}"),
    }
}

async fn raw_list(url: &str, names: &Names, messages: usize) -> Sample {
    let consuming_pool = pool(url, POOL).await;

    let run = Run::new(messages);
    let consuming = tokio::spawn({
        let pool = consuming_pool.clone();
        let names = names.clone();
        let processing = names.processing();
        let run = run.clone();
        async move {
            loop {
                let popped = empty_on_timeout(
                    pool.blmove(
                        names.key.as_str(),
                        processing.as_str(),
                        LMoveDirection::Right,
                        LMoveDirection::Left,
                        BLOCK_SECS,
                    )
                    .await,
                );
                let Some(value) = popped else { continue };
                let order: Order = serde_json::from_slice(&value).expect("the body decodes");
                black_box((order.id, order.quantity));
                let done = run.arrived();
                let _: i64 = pool
                    .lrem(processing.as_str(), 1, value)
                    .await
                    .expect("the entry leaves the processing list");
                if done {
                    break;
                }
            }
        }
    });

    let publishing = pool(url, 1).await;
    publish_list(&publishing, &names.key, messages, &run).await;
    drain(&run, "raw list").await;
    consuming.await.expect("the consuming task ends");

    let sample = Sample {
        window: run.window(),
    };
    unlink(&publishing, &[&names.key, &names.processing()]).await;
    close(publishing).await;
    close(consuming_pool).await;
    sample
}

/// The dedicated connection a Pub/Sub subscription reads on, opened the way this crate opens it:
/// a second client built from the same config, with nothing configured on top.
async fn subscriber_client(url: &str) -> Client {
    let config = Config::from_url(url).expect("the URL is a Redis URL");
    let client = Client::new(config, None, None, None);
    client
        .init()
        .await
        .expect("the server accepts a connection");
    client
}

async fn raw_pubsub(url: &str, names: &Names, messages: usize) -> Sample {
    // The pool a service holds even when its subscription does not read through it, so every loop
    // keeps the same connections open to the same server.
    let idle = pool(url, POOL).await;
    let client = subscriber_client(url).await;
    // Opened before the subscribe, as this crate opens it: the receiver sees only what is sent
    // after it exists.
    let mut rx = client.message_rx();
    client
        .subscribe(names.key.as_str())
        .await
        .expect("the server accepts the subscribe");

    let run = Run::new(messages);
    let consuming = tokio::spawn({
        let run = run.clone();
        async move {
            loop {
                match rx.recv().await {
                    Ok(message) => {
                        let raw = message.value.as_bytes().unwrap_or(&[]);
                        let order: Order = serde_json::from_slice(raw).expect("the body decodes");
                        black_box((order.id, order.quantity));
                        if run.arrived() {
                            break;
                        }
                    }
                    // The receiver fell behind the client's broadcast buffer; skip the gap and
                    // keep reading, which is what this crate's own stream does.
                    Err(RecvError::Lagged(_)) => {}
                    Err(RecvError::Closed) => break,
                }
            }
        }
    });

    let publishing = pool(url, 1).await;
    publish_pubsub(&publishing, &names.key, &run).await;
    drain(&run, "raw pubsub").await;
    consuming.await.expect("the consuming task ends");

    let sample = Sample {
        window: run.window(),
    };
    close(publishing).await;
    close(client).await;
    close(idle).await;
    sample
}

async fn framework_stream(url: &str, names: &Names, messages: usize) -> Sample {
    let run = Run::new(messages);
    install(names);
    let app = start_stream(url, run.clone()).await;
    let publishing = pool(url, 1).await;
    publish_stream(&publishing, &names.key, messages, &run).await;
    drain(&run, "framework stream").await;
    app.shutdown().await.expect("the service stops");

    let sample = Sample {
        window: run.window(),
    };
    unlink(&publishing, &[&names.key]).await;
    close(publishing).await;
    sample
}

async fn framework_pubsub(url: &str, names: &Names, messages: usize) -> Sample {
    let run = Run::new(messages);
    install(names);
    let app = start_pubsub(url, run.clone()).await;
    let publishing = pool(url, 1).await;
    publish_pubsub(&publishing, &names.key, &run).await;
    drain(&run, "framework pubsub").await;
    app.shutdown().await.expect("the service stops");

    let sample = Sample {
        window: run.window(),
    };
    close(publishing).await;
    sample
}

async fn framework_list(url: &str, names: &Names, messages: usize) -> Sample {
    let run = Run::new(messages);
    install(names);
    let app = start_list(url, run.clone()).await;
    let publishing = pool(url, 1).await;
    publish_list(&publishing, &names.key, messages, &run).await;
    drain(&run, "framework list").await;
    app.shutdown().await.expect("the service stops");

    let sample = Sample {
        window: run.window(),
    };
    unlink(&publishing, &[&names.key, &names.processing()]).await;
    close(publishing).await;
    sample
}

// ---------------------------------------------------------------------------------------------
// The run
// ---------------------------------------------------------------------------------------------

/// The scenarios a run measures, in the order the page publishes them.
const SCENARIOS: [Scenario; 3] = [Scenario::Stream, Scenario::List, Scenario::PubSub];

#[derive(Clone, Copy, Debug)]
enum Scenario {
    Stream,
    List,
    PubSub,
}

impl Scenario {
    fn name(self) -> &'static str {
        match self {
            Self::Stream => "Redis Streams consumer group, 512 B JSON, ack each",
            Self::List => "Redis list work queue (reliable), 512 B JSON, ack each",
            Self::PubSub => "Redis Pub/Sub, 512 B JSON, no acknowledgement",
        }
    }

    /// Round trips a delivery costs the consumer, which is what decides whether the run was paced
    /// by the server rather than by the code being measured.
    ///
    /// A stream delivery is one `XACK`, plus a sixty-fourth of the read that fetched it. A
    /// reliable list entry is two: the `BLMOVE` that claims it and the `LREM` that settles it.
    /// Pub/Sub settles nothing and reads nothing, so a delivery costs none.
    fn round_trips(self) -> f64 {
        match self {
            Self::Stream => 1.0 + 1.0 / PREFETCH as f64,
            Self::List => 2.0,
            Self::PubSub => 0.0,
        }
    }

    async fn raw(self, url: &str, names: &Names, messages: usize) -> Sample {
        match self {
            Self::Stream => raw_stream(url, names, messages).await,
            Self::List => raw_list(url, names, messages).await,
            Self::PubSub => raw_pubsub(url, names, messages).await,
        }
    }

    /// The loop that drives this crate's own consumer.
    async fn adapter(self, url: &str, names: &Names, messages: usize) -> Sample {
        match self {
            Self::Stream => adapter_stream(url, names, messages).await,
            Self::List => adapter_list(url, names, messages).await,
            Self::PubSub => adapter_pubsub(url, names, messages).await,
        }
    }

    /// The service a user writes, started through the real runtime.
    async fn framework(self, url: &str, names: &Names, messages: usize) -> Sample {
        match self {
            Self::Stream => framework_stream(url, names, messages).await,
            Self::List => framework_list(url, names, messages).await,
            Self::PubSub => framework_pubsub(url, names, messages).await,
        }
    }
}

/// Median, smallest and largest of the kept pairs.
#[derive(Clone, Copy, Debug)]
struct Stats {
    median: f64,
    min: f64,
    max: f64,
}

impl Stats {
    fn of(mut rates: Vec<f64>) -> Self {
        rates.sort_by(f64::total_cmp);
        let middle = rates.len() / 2;
        let median = if rates.len().is_multiple_of(2) {
            f64::midpoint(rates[middle - 1], rates[middle])
        } else {
            rates[middle]
        };
        Self {
            median,
            min: rates[0],
            max: rates[rates.len() - 1],
        }
    }

    fn spread(self) -> f64 {
        self.max - self.min
    }
}

#[derive(Debug)]
struct Measured {
    scenario: Scenario,
    messages: usize,
    rounds: usize,
    raw: Stats,
    adapter: Stats,
    framework: Stats,
    /// The service against the client: what a reader pays over writing the loop by hand.
    overhead_percent: f64,
    verdict: &'static str,
    /// This crate's consumer against the client it wraps: the number this repository owns.
    adapter_overhead_percent: f64,
    adapter_verdict: &'static str,
    broker_bound: bool,
}

/// The methodology's honesty rule: a difference smaller than the run-to-run spread is a verdict,
/// never a percentage.
fn verdict(baseline: Stats, other: Stats) -> &'static str {
    if (baseline.median - other.median).abs() < baseline.spread().max(other.spread()) {
        "indistinguishable"
    } else {
        "measured"
    }
}

fn overhead(baseline: Stats, other: Stats) -> f64 {
    (baseline.median - other.median) / baseline.median * 100.0
}

/// What one command costs against this server, measured on a connection of its own.
///
/// Sent sequentially on one connection, which is how a consumer sends the read and the
/// acknowledgement its delivery needs, and against a key that does not exist, so the figure is the
/// round trip and not the work behind it.
async fn round_trip(url: &str) -> Duration {
    let probe = pool(url, 1).await;
    let key = format!("{}:rtt", Names::fresh().key);
    let started = Instant::now();
    for _ in 0..ROUND_TRIP_PROBES {
        let _: i64 = probe
            .exists(key.as_str())
            .await
            .expect("the server answers");
    }
    let each = started.elapsed() / ROUND_TRIP_PROBES as u32;
    close(probe).await;
    each
}

async fn measure(
    scenario: Scenario,
    url: &str,
    rounds: usize,
    seconds: f64,
    round_trip: Duration,
) -> Measured {
    // The probe is the warm-up as well: its result is thrown away, and the rate it measured sets
    // a count that makes every run below last at least `seconds`.
    let probe = scenario.raw(url, &Names::fresh(), PROBE_MESSAGES).await;
    let messages = ((probe.rate(PROBE_MESSAGES) * seconds * MARGIN) as usize)
        .clamp(PROBE_MESSAGES, MAX_MESSAGES);
    println!(
        "{}: {messages} messages per run ({:.0} msg/s probed)",
        scenario.name(),
        probe.rate(PROBE_MESSAGES)
    );

    let mut raws = Vec::with_capacity(rounds);
    let mut adapters = Vec::with_capacity(rounds);
    let mut frameworks = Vec::with_capacity(rounds);
    for round in 0..=rounds {
        // Interleaved, never blocked: running one loop to the end and then the next would charge
        // every drift of the machine to whichever ran last.
        let raw = scenario.raw(url, &Names::fresh(), messages).await;
        let adapter = scenario.adapter(url, &Names::fresh(), messages).await;
        let framework = scenario.framework(url, &Names::fresh(), messages).await;
        if round == 0 {
            continue;
        }
        println!(
            "  round {round:>2}: raw {:>10.0} msg/s, adapter {:>10.0} msg/s, framework {:>10.0} \
             msg/s",
            raw.rate(messages),
            adapter.rate(messages),
            framework.rate(messages)
        );
        raws.push(raw.rate(messages));
        adapters.push(adapter.rate(messages));
        frameworks.push(framework.rate(messages));
    }

    let raw = Stats::of(raws);
    let adapter = Stats::of(adapters);
    let framework = Stats::of(frameworks);
    Measured {
        scenario,
        messages,
        rounds,
        raw,
        adapter,
        framework,
        overhead_percent: overhead(raw, framework),
        verdict: verdict(raw, framework),
        adapter_overhead_percent: overhead(raw, adapter),
        adapter_verdict: verdict(raw, adapter),
        // What a delivery costs the server in round trips against what it cost in total: a
        // consumer that spent most of its window waiting on the socket was paced by the server,
        // and everything above the socket does its work inside that wait.
        broker_bound: broker_bound(scenario, raw.median, round_trip),
    }
}

/// Whether the raw loop spent most of the run waiting on the server rather than working.
fn broker_bound(scenario: Scenario, raw_rate: f64, round_trip: Duration) -> bool {
    let per_message = 1.0 / raw_rate;
    let waiting = scenario.round_trips() * round_trip.as_secs_f64();
    waiting >= per_message / 2.0
}

fn document(measured: &[Measured]) -> String {
    let mut out = String::from("{\n  \"scenarios\": [\n");
    for (index, row) in measured.iter().enumerate() {
        let comma = if index + 1 == measured.len() { "" } else { "," };
        write!(
            out,
            concat!(
                "    {{\n",
                "      \"name\": \"{name}\",\n",
                "      \"unit\": \"msg/s\",\n",
                "      \"messages\": {messages},\n",
                "      \"pairs\": {rounds},\n",
                "      \"raw\": {{ \"median\": {raw_median:.0}, \"min\": {raw_min:.0},",
                " \"max\": {raw_max:.0} }},\n",
                "      \"adapter\": {{ \"median\": {ad_median:.0}, \"min\": {ad_min:.0},",
                " \"max\": {ad_max:.0} }},\n",
                "      \"framework\": {{ \"median\": {fw_median:.0}, \"min\": {fw_min:.0},",
                " \"max\": {fw_max:.0} }},\n",
                "      \"overhead_percent\": {overhead:.1},\n",
                "      \"verdict\": \"{verdict}\",\n",
                "      \"adapter_overhead_percent\": {adapter_overhead:.1},\n",
                "      \"adapter_verdict\": \"{adapter_verdict}\",\n",
                "      \"broker_bound\": {broker_bound}\n",
                "    }}{comma}\n",
            ),
            name = row.scenario.name(),
            messages = row.messages,
            rounds = row.rounds,
            raw_median = row.raw.median,
            raw_min = row.raw.min,
            raw_max = row.raw.max,
            ad_median = row.adapter.median,
            ad_min = row.adapter.min,
            ad_max = row.adapter.max,
            fw_median = row.framework.median,
            fw_min = row.framework.min,
            fw_max = row.framework.max,
            overhead = row.overhead_percent,
            verdict = row.verdict,
            adapter_overhead = row.adapter_overhead_percent,
            adapter_verdict = row.adapter_verdict,
            broker_bound = row.broker_bound,
            comma = comma,
        )
        .expect("writing to a String");
    }
    out.push_str("  ]\n}\n");
    out
}

fn runtime() -> Runtime {
    Builder::new_multi_thread()
        .worker_threads(WORKERS)
        .enable_all()
        .build()
        .expect("the tokio runtime builds")
}

fn number(name: &str, fallback: usize) -> usize {
    env::var(name).ok().map_or(fallback, |value| {
        value
            .parse()
            .unwrap_or_else(|_| panic!("{name} must be a positive number"))
    })
}

/// Turns the server's append-only file off for the run. What is measured is the cost of a
/// delivery, and an `fsync` that lands inside one loop of a round belongs to none of them.
async fn silence_the_disk(url: &str) {
    let admin = pool(url, 1).await;
    admin
        .config_set("appendonly", "no")
        .await
        .expect("the server takes the setting");
    close(admin).await;
}

fn main() {
    let url = env::var("REDIS_TEST_URL")
        .expect("REDIS_TEST_URL names the server to measure against; `just bench` sets it");
    let rounds = number("RUSTSTREAM_BENCH_ROUNDS", ROUNDS);
    let seconds = number("RUSTSTREAM_BENCH_SECONDS", SECONDS as usize) as f64;
    let out = env::var("RUSTSTREAM_BENCH_OUT").unwrap_or_else(|_| "bench-paired.json".to_owned());

    let runtime = runtime();
    runtime.block_on(silence_the_disk(&url));
    let round_trip = runtime.block_on(round_trip(&url));
    println!("one command to this server costs {round_trip:?}\n");
    let measured: Vec<Measured> = SCENARIOS
        .into_iter()
        .map(|scenario| runtime.block_on(measure(scenario, &url, rounds, seconds, round_trip)))
        .collect();

    println!();
    for row in &measured {
        println!(
            "{}: raw {:.0} msg/s, adapter {:.0} msg/s ({:.1}%, {}), framework {:.0} msg/s ({:.1}%, {}){}",
            row.scenario.name(),
            row.raw.median,
            row.adapter.median,
            row.adapter_overhead_percent,
            row.adapter_verdict,
            row.framework.median,
            row.overhead_percent,
            row.verdict,
            if row.broker_bound {
                ", broker-bound"
            } else {
                ""
            }
        );
    }

    std::fs::write(&out, document(&measured)).expect("the summary is written");
    println!("\nwrote {out}");
}
