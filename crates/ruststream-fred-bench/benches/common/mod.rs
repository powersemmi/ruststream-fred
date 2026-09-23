//! Shared parts of the crate's code-cost benchmarks: what is measured, and how the measurement is
//! kept to one region.
//!
//! # What a scenario looks like
//!
//! One scenario per file, and this module carries what they have in common: the payload, the
//! service setup, the fill, the latch a handler counts deliveries down on, and the measurement
//! configuration. The method is the core's, described in its `benches/common` and on the
//! [RustStream benchmarks page](https://powersemmi.github.io/ruststream/latest/benchmarks/).
//!
//! A scenario runs the service a user writes: the app, built on [`RedisBroker::standalone`] and
//! started through [`RustStream::start`], against the standalone server of the compose stand
//! (`REDIS_TEST_URL`, or [`DEFAULT_ADDRESS`] when it is not set). The subscription is a
//! [`RedisStream`](ruststream_fred::RedisStream) consumer group and a reply goes through
//! [`RedisPublish`](ruststream_fred::RedisPublish), the connected broker's default policy, so
//! every command a scenario sends is one a service sends: `XREADGROUP` to read, `XACK` to settle,
//! `XADD` to reply.
//!
//! # What is counted
//!
//! The service runs on a single-threaded tokio runtime, and `fred` runs on it too: the tasks the
//! client spawns for its connections are driven on the same thread. Everything on that thread
//! inside the measured region is counted: the dispatcher, the codec, this crate's code, and
//! `fred` writing the commands and parsing the replies. The server is another process, and the
//! kernel's side of a socket call is not an instruction of this one, so neither is in the number.
//!
//! Collection starts switched off and is switched on for [`measure`], which every body wraps its
//! work in. [`measure`] is the only frame that carries its name, because a toggle on a name that
//! also appears inside closure types switches collection off again one frame deeper. DHAT is
//! pointed at the same frame; the number read is `Total blocks`, allocations per run.
//!
//! # Steady state and cold start
//!
//! Every scenario is measured over one delivery, over [`MESSAGES`] deliveries and over twice as
//! many. The slope between the last two is the steady-state cost of a message: everything that
//! happens once is in both totals and cancels in the subtraction. The one-delivery run is the
//! cold start, reported on its own: connecting the pool, creating the consumer group, opening
//! the subscription and taking the first delivery.
//!
//! What a body measures is the start and the drain, in two regions. Between them a second thread,
//! with a runtime and a `fred` client of its own, appends the messages to the stream in pipelined
//! batches and waits until the server has accepted every one. The service's runtime is not driven
//! while it does, so nothing is consumed before the drain starts, and the fill is in neither
//! region. Every run starts from a clean server: the setup unlinks the input and reply streams,
//! and the consumer group with them, before the service is built.

// Each benchmark target compiles this module on its own and uses the part it needs; what another
// target uses looks unused here.
#![allow(dead_code)]

use std::convert::Infallible;
use std::env;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

use fred::clients::Client;
use fred::interfaces::{ClientLike, KeysInterface, StreamsInterface};
use fred::types::Value;
use fred::types::config::Config;
use gungraun::{Callgrind, Dhat, DhatMetric, EntryPoint, EventKind, LibraryBenchmarkConfig};
use ruststream::runtime::{AppInfo, BrokerScope, Identity, RunningApp, RustStream};
use ruststream_fred::RedisBroker;
use serde::Deserialize;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::Notify;

// A benchmark measures what ships. The framework's harness feature swaps the broker for an
// in-process stand-in, so a count taken with it on is not this crate's transport at all.
#[cfg(feature = "testing")]
compile_error!(
    "benchmarks must be built without the `testing` feature; run them through `just bench-code`"
);

/// The stream key every scenario delivers on, and the one each handler's descriptor names.
pub const INPUT: &str = "orders";

/// The stream key the reply scenario's reply type names in its `#[outgoing(..)]` attribute,
/// which takes a literal; it is unlinked with [`INPUT`] before a run.
pub const REPLIES: &str = "confirmations";

/// The variable `just bench-code` sets to the standalone server, the one the live suites read.
const ADDRESS_VARIABLE: &str = "REDIS_TEST_URL";

/// The standalone server of the compose stand, where `just bench-code` reaches it.
pub const DEFAULT_ADDRESS: &str = "redis://127.0.0.1:6379";

/// The field a stream entry carries its body in, and the field this crate reads it from.
const PAYLOAD_FIELD: &str = "_payload";

/// Entries per pipelined fill batch: the fill is not the subject, and one round trip per entry
/// would make a run wait on the loopback for nothing.
const FILL_BATCH: usize = 256;

/// The values every body carries. Fixed, so that every delivery of a run costs the same.
const ID: u64 = 1_000_000;
const QUANTITY: u32 = 37;

/// The payload every scenario decodes: two integer fields, so a decode allocates nothing and the
/// number is about the crate and the framework rather than about `serde_json`'s string handling.
#[derive(Debug, Deserialize)]
pub struct Order {
    pub id: u64,
    pub quantity: u32,
}

/// Deliveries per measured run: large enough that entering and leaving the region is lost in the
/// per-message number, small enough that a scenario stays within seconds of valgrind time.
/// `scripts/bench_results.py` divides by the same count.
pub const MESSAGES: usize = 1_000;

/// The measurement configuration every gated scenario shares.
///
/// `steady` per delivery and `cold` once are the hard limit the longest run of the scenario
/// (twice [`MESSAGES`] deliveries) is held to, so the run fails when the path allocates more
/// than it does today. Against a real server a count moves by a few blocks between runs, so each
/// scenario states its floor as the highest total it reached plus a margin; a number that goes
/// down is lowered there in the same change. The instruction limit is relative:
/// `just bench-code --save-baseline=main` records a baseline and `just bench-code
/// --baseline=main` compares against it.
pub fn config(steady: u64, cold: u64) -> LibraryBenchmarkConfig {
    config_every(steady, 1, cold)
}

/// The same for a scenario whose allocations do not come one per delivery: `steady` blocks per
/// `per` deliveries, as a batch handler allocates per batch.
pub fn config_every(steady: u64, per: u64, cold: u64) -> LibraryBenchmarkConfig {
    let mut config = LibraryBenchmarkConfig::default();
    config
        .pass_through_env(ADDRESS_VARIABLE)
        .tool(callgrind().soft_limits([(EventKind::Ir, 2f64)]))
        .tool(dhat().hard_limits([(DhatMetric::TotalBlocks, blocks(steady, per, cold))]));
    config
}

/// The limit for the configured count: the cold part once, plus the steady rate over the longest
/// run of the scenario, which is twice [`MESSAGES`]. The division rounds up.
const fn blocks(steady: u64, per: u64, cold: u64) -> u64 {
    cold + (steady * 2 * MESSAGES as u64).div_ceil(per)
}

/// Callgrind collecting inside the measured region alone.
fn callgrind() -> Callgrind {
    let mut callgrind = Callgrind::with_args([
        "--collect-atstart=no",
        &format!("--toggle-collect={REGION}"),
    ]);
    callgrind.entry_point(EntryPoint::None);
    callgrind
}

/// The measured region: everything this runs is counted, nothing around it is.
#[inline(never)]
pub fn measure<T>(body: impl FnOnce() -> T) -> T {
    body()
}

/// DHAT with a stack window deep enough to reach the measured frame from a publish inside a
/// dispatched handler.
fn dhat() -> Dhat {
    let mut dhat = Dhat::with_args(["--num-callers=128"]);
    dhat.entry_point(EntryPoint::Custom(REGION.to_owned()));
    dhat
}

/// The frame both tools are pointed at.
const REGION: &str = "*common::measure*";

/// A single-threaded runtime: one thread means one order of execution on the service's side.
pub fn runtime() -> Runtime {
    Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current-thread runtime")
}

/// Where the stand's standalone server is.
fn address() -> String {
    env::var(ADDRESS_VARIABLE)
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_ADDRESS.to_owned())
}

/// Counts deliveries down and wakes the benchmark body when the last one has been handled.
///
/// Handlers reach it as the application state. What a delivery pays for it is one relaxed
/// decrement and the branch that reads it.
#[derive(Clone, Debug)]
pub struct Latch(Arc<Inner>);

#[derive(Debug)]
struct Inner {
    remaining: AtomicUsize,
    drained: Notify,
}

impl Default for Latch {
    fn default() -> Self {
        Self(Arc::new(Inner {
            remaining: AtomicUsize::new(0),
            drained: Notify::new(),
        }))
    }
}

impl Latch {
    /// Arms the latch for `count` deliveries.
    pub fn expect(&self, count: usize) {
        self.0.remaining.store(count, Ordering::Release);
    }

    /// Records one handled delivery, waking the waiter on the last one.
    pub fn arrived(&self) {
        if self.0.remaining.fetch_sub(1, Ordering::Relaxed) == 1 {
            self.0.drained.notify_one();
        }
    }

    /// How many deliveries the latch is still waiting for.
    pub fn remaining(&self) -> usize {
        self.0.remaining.load(Ordering::Acquire)
    }

    /// Resolves once every expected delivery has been handled.
    pub async fn drained(&self) {
        while self.0.remaining.load(Ordering::Acquire) > 0 {
            self.0.drained.notified().await;
        }
    }
}

/// The JSON body every delivery carries: the two fields a handler reads.
pub fn json_body() -> Vec<u8> {
    format!("{{\"id\":{ID},\"quantity\":{QUANTITY}}}").into_bytes()
}

/// A service that is built but not started, and how many messages its stream will get.
///
/// The start is part of the measurement rather than of the setup, because the cold number is
/// what starting costs. It is held as a boxed call so that every scenario hands over the same
/// type; the one indirect call it adds lands in the cold number and nowhere else.
pub struct Pending {
    runtime: Runtime,
    latch: Latch,
    start: Box<dyn FnOnce(&Runtime) -> RunningApp>,
    messages: usize,
}

/// The mount a scenario passes in: what `with_broker` does with the scope.
pub type Mount<'a> = &'a mut BrokerScope<RedisBroker, Identity, (), Latch>;

/// Builds a one-handler service on the production broker, ready to be started by the body.
///
/// The server is cleaned first, so a run never reads what the one before it left behind.
pub fn pending(messages: usize, mount: impl FnOnce(Mount<'_>)) -> Pending {
    aside(async move |client: Client| {
        for key in [INPUT, REPLIES] {
            let _: i64 = client.unlink(key).await.expect("the key is unlinked");
        }
    });
    let latch = Latch::default();
    let state = latch.clone();
    let app = RustStream::new(AppInfo::new("bench", "0.0.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(state))
        .with_broker(RedisBroker::standalone(address()), mount);
    Pending {
        runtime: runtime(),
        latch,
        start: Box::new(move |runtime| runtime.block_on(app.start()).expect("the service starts")),
        messages,
    }
}

/// Runs `work` on a thread of its own, with a runtime and a `fred` client of its own, and waits
/// until it is done.
///
/// Nothing this does is on the service's thread, so none of it is counted, and the service's
/// runtime is not driven meanwhile.
fn aside(work: impl AsyncFnOnce(Client) + Send) {
    thread::scope(|scope| {
        scope
            .spawn(move || {
                runtime().block_on(async move {
                    let config = Config::from_url(&address()).expect("a Redis URL");
                    let client = Client::new(config, None, None, None);
                    let _connection = client
                        .init()
                        .await
                        .expect("the stand's server accepts a connection");
                    work(client.clone()).await;
                    client.quit().await.expect("the connection closes");
                });
            })
            .join()
            .expect("the side thread finishes");
    });
}

/// Appends `count` bodies to [`INPUT`] in pipelined batches, and returns once the server has
/// accepted every one of them.
fn fill(count: usize) {
    aside(async move |client: Client| {
        let body = json_body();
        let mut sent = 0;
        while sent < count {
            let batch = FILL_BATCH.min(count - sent);
            let pipeline = client.pipeline();
            for _ in 0..batch {
                let fields = vec![(PAYLOAD_FIELD.to_owned(), body.clone())];
                let _: () = pipeline
                    .xadd(INPUT, false, None::<()>, "*", fields)
                    .await
                    .expect("the entry is queued");
            }
            let _: Vec<Value> = pipeline
                .all()
                .await
                .expect("the server accepts the entries");
            sent += batch;
        }
    });
}

/// Starts the service, fills its stream, and drains it: the shape of every scenario here.
///
/// Two measured regions, and the fill between them is in neither. The first is the cold start,
/// the second the deliveries. The service is shut down after the second, outside both.
pub fn start_and_drain(pending: Pending) {
    let Pending {
        runtime,
        latch,
        start,
        messages,
    } = pending;
    let running = measure(|| start(&runtime));
    latch.expect(messages);
    fill(messages);
    assert_eq!(
        latch.remaining(),
        messages,
        "the stream was consumed while it was being filled, so the measured region would be short"
    );
    measure(|| runtime.block_on(latch.drained()));
    runtime
        .block_on(running.shutdown())
        .expect("the service shuts down");
}
