//! A `.pipeline()` list subscription: a reliable list's `LREM` settles ride the window beside
//! what its handlers queued, and a simple list's window carries the handlers' commands alone.

#![cfg(feature = "testing")]

use ruststream::testing::TestApp;
use ruststream_fred::context::keys;
use ruststream_fred::pipeline::RedisPipeline;
use ruststream_fred::prelude::*;
use ruststream_fred::testing::RedisTestBroker;
use ruststream_fred::{AtomicList, PipelinedList};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Outgoing, PartialEq, Serialize)]
struct Job {
    id: u64,
    keep: bool,
}

async fn queue_and_settle(job: &Job, pipeline: &RedisPipeline) -> HandlerOutcome {
    let body = serde_json::to_vec(job).expect("a job encodes");
    if pipeline.lpush("audit", body).await.is_err() {
        return HandlerOutcome::retry();
    }
    if job.keep {
        HandlerOutcome::ack()
    } else {
        HandlerOutcome::drop()
    }
}

#[subscriber(PipelinedList::new("jobs").reliable())]
async fn reliable(job: &Job, Ctx(pipeline): Ctx<keys::Pipeline>) -> HandlerOutcome {
    queue_and_settle(job, &pipeline).await
}

#[subscriber(PipelinedList::new("jobs"))]
async fn simple(job: &Job, Ctx(pipeline): Ctx<keys::Pipeline>) -> HandlerOutcome {
    queue_and_settle(job, &pipeline).await
}

#[subscriber(AtomicList::new("jobs").reliable())]
async fn atomic(job: &Job, Ctx(pipeline): Ctx<keys::Pipeline>) -> HandlerOutcome {
    queue_and_settle(job, &pipeline).await
}

macro_rules! case {
    ($test:ident, $handler:ident, $keep:literal, $expected:literal) => {
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn $test() {
            let app = RustStream::new(AppInfo::new("pipeline", "0.1.0")).with_broker(
                RedisTestBroker::new(),
                |b| {
                    b.include($handler);
                },
            );
            let tb = TestApp::start(app).await.expect("start");

            tb.broker::<RedisTestBroker>()
                .message(&Job { id: 1, keep: $keep })
                .to("jobs")
                .publish()
                .await
                .expect("publish");

            tb.broker::<RedisTestBroker>()
                .subscriber("jobs")
                .assert_called_once();
            tb.broker::<RedisTestBroker>()
                .published::<Job>("audit")
                .assert_called($expected);

            tb.shutdown().await.expect("shutdown");
        }
    };
}

case!(
    a_reliable_job_acknowledged_sends_what_it_queued,
    reliable,
    true,
    1
);
case!(
    a_reliable_job_dropped_sends_nothing_it_queued,
    reliable,
    false,
    0
);
case!(
    a_simple_job_acknowledged_sends_what_it_queued,
    simple,
    true,
    1
);
case!(
    a_simple_job_dropped_sends_nothing_it_queued,
    simple,
    false,
    0
);
case!(
    an_atomic_job_acknowledged_sends_what_it_queued,
    atomic,
    true,
    1
);
case!(
    an_atomic_job_dropped_sends_nothing_it_queued,
    atomic,
    false,
    0
);
