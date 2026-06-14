//! Redis list work queue: competing consumers pop jobs with `BRPOP` (simple) or `LMOVE` (reliable).
//!
//! A producer `LPUSH`es jobs; each job goes to exactly one consumer (no fan-out). Simple mode is
//! at-most-once (ack unsupported). Reliable mode moves the job to a processing list and removes it
//! on ack, so a crashed handler's job is not silently lost.
//!
//! ```text
//! cargo run --example fred_list --features macros,json -- run
//! ```
//!
//! Enqueue a job from another terminal:
//!
//! ```text
//! redis-cli LPUSH jobs '{"id":1}'
//! ```

use std::time::Duration;

use ruststream::runtime::{AppInfo, HandlerResult, RustStream};
use ruststream::subscriber;
use ruststream_fred::{RedisBroker, RedisList};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Job {
    id: u64,
}

// --8<-- [start:simple]
// Simple at-most-once work queue: BRPOP, no ack.
#[subscriber(RedisList::new("jobs"))]
async fn run_job(job: &Job) -> HandlerResult {
    println!("running job {}", job.id);
    HandlerResult::Ack
}
// --8<-- [end:simple]

// --8<-- [start:reliable]
// Reliable at-least-once work queue: the job moves to a processing list and is removed on ack.
#[subscriber(RedisList::new("jobs.reliable").reliable())]
async fn run_reliable_job(job: &Job) -> HandlerResult {
    println!("running reliable job {}", job.id);
    HandlerResult::Ack
}
// --8<-- [end:reliable]

// --8<-- [start:recovery]
// Reliable mode with orphan recovery: a dead consumer's in-flight job is returned to the queue. The
// recovery ZSET key is named explicitly, and min_idle must exceed the longest legitimate handler
// runtime (set it too low and a still-running job gets recovered and processed twice).
#[subscriber(
    RedisList::new("jobs.recoverable")
        .reliable()
        .min_idle(Duration::from_secs(30))
        .recovery_zset("jobs.recoverable.inflight")
)]
async fn run_recoverable_job(job: &Job) -> HandlerResult {
    println!("running recoverable job {}", job.id);
    HandlerResult::Ack
}
// --8<-- [end:recovery]

#[ruststream::app]
fn app() -> RustStream {
    RustStream::new(AppInfo::new("jobs", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            b.include(run_job);
            b.include(run_reliable_job);
            b.include(run_recoverable_job);
        },
    )
}
