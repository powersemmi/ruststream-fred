//! Dead-letter routing and a poison cap on a reliable Redis List: a job that keeps failing is moved
//! to a dead-letter list instead of being redelivered forever or silently dropped.
//!
//! ```text
//! cargo run --example fred_list_dead_letter --features macros,json -- run
//! ```
//!
//! Enqueue a poison job from another terminal (id 0 keeps failing until the cap dead-letters it):
//!
//! ```text
//! redis-cli LPUSH jobs.dlq '{"id":0}'
//! ```

use ruststream::runtime::{AppInfo, HandlerResult, RustStream};
use ruststream::subscriber;
use ruststream_fred::{RedisBroker, RedisList};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Job {
    id: u64,
}

// --8<-- [start:handler]
// Cap redeliveries at 5. On the 5th failed delivery (or an explicit drop) the job is LPUSH-ed to
// the "jobs.failed" list, tagged with the reason, rather than retried forever or discarded.
#[subscriber(
    RedisList::new("jobs.dlq")
        .reliable()
        .dead_letter("jobs.failed")
        .max_deliveries(5)
)]
async fn handle_job(job: &Job) -> HandlerResult {
    if job.id == 0 {
        // A poison job: nack to retry. Once the cap is reached it is dead-lettered for you.
        return HandlerResult::Nack { requeue: true };
    }
    println!("processed job {}", job.id);
    HandlerResult::Ack
}
// --8<-- [end:handler]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> RustStream {
    RustStream::new(AppInfo::new("jobs", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            b.include(handle_job);
        },
    )
}
// --8<-- [end:app]
