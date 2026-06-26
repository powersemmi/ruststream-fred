//! Domain types and handlers, written as `#[subscriber]` functions.
//!
//! The first parameter is the decoded payload; the macro turns each function into a mountable
//! definition (a value named after the function) that `routes` collects into a `Router`. `run_job`
//! binds to a [`RedisList`] key and consumes one job per popped entry. Simple (default) lists are
//! at-most-once `BRPOP` with no acknowledgement; switch to `RedisList::new("jobs").reliable()` for
//! at-least-once delivery where the entry is removed only on `Ack`.

use ruststream::runtime::HandlerResult;
use ruststream::subscriber;
use ruststream_fred::RedisList;
use schemars::JsonSchema;
use serde::Deserialize;

/// A job enqueued on the `jobs` list.
///
/// `JsonSchema` lets `asyncapi gen` emit this payload's schema into the generated document.
#[derive(Debug, Deserialize, JsonSchema)]
pub struct Job {
    pub id: u64,
    pub task: String,
}

/// Runs each job popped off the `jobs` queue. A simple list cannot ack, so this is a consume-only
/// handler: it does its work and returns `Ack` to mark the entry handled.
#[subscriber(RedisList::new("jobs"))]
pub async fn run_job(job: &Job) -> HandlerResult {
    println!("running job {} ({})", job.id, job.task);
    HandlerResult::Ack
}
