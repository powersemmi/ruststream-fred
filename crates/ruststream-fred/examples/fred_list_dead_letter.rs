//! Capping the retries on a reliable Redis List: a job that keeps failing goes to a dead-letter
//! list instead of being redelivered forever or silently dropped.
//!
//! ```text
//! cargo run --example fred_list_dead_letter --features macros,json -- run
//! ```
//!
//! Enqueue a poison job from another terminal (id 0 keeps failing until the cap moves it):
//!
//! ```text
//! redis-cli LPUSH jobs '{"id":0}'
//! ```

use ruststream_fred::list::prelude::*;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Job {
    id: u64,
}

// --8<-- [start:handler]
#[subscriber(RedisList::new("jobs").reliable())]
async fn handle_job(job: &Job) -> HandlerOutcome {
    if job.id == 0 {
        // A poison job: ask for a retry. Once the attempts are spent it is carried away for you.
        return HandlerOutcome::retry();
    }
    println!("processed job {}", job.id);
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("jobs", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            // Five deliveries per job, counting the first. The fifth failure `LPUSH`es it onto
            // "jobs.failed" instead of back onto "jobs".
            b.include(handle_job)
                .max_attempts(nonzero!(5u32))
                .dead_letter("jobs.failed");
        },
    )
}
// --8<-- [end:app]
