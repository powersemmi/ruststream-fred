//! Redis lists as work queues: the simple form pops an entry and settles nothing, the reliable
//! form keeps it on a processing list until the handler settles it, and a recovery set returns
//! what a consumer left there.
//!
//! ```text
//! cargo test --example redis_lists --features testing
//! cargo run --example redis_lists --features testing -- run
//! ```

use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use ruststream_fred::list::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Job {
    id: u64,
}

// A stand-in for the render farm the jobs go to: it turns away the first job it is sent while it
// warms up, and takes every job after that. Wired once at startup, injected through `State`.
#[derive(Clone, Default)]
struct RenderFarm {
    warm: Arc<AtomicBool>,
}

impl RenderFarm {
    async fn render(&self, job: &Job) -> Result<(), String> {
        tokio::task::yield_now().await;
        if self.warm.swap(true, Ordering::Relaxed) {
            Ok(())
        } else {
            Err(format!("render farm warming up, job {}", job.id))
        }
    }
}

// `#[derive(FromRef)]` makes each state field injectable with `State<FieldType>`.
#[derive(FromRef)]
struct AppState {
    farm: RenderFarm,
}

/// How long a claimed invoice may sit unsettled before the watchdog returns it to the queue.
const MIN_IDLE: Duration = Duration::from_secs(60);

// --8<-- [start:simple]
/// Each entry of `thumbnails` reaches one consumer. It left Redis when it was popped, so the
/// outcome settles nothing.
#[subscriber(RedisList::new("thumbnails"))]
async fn make_thumbnail(job: &Job) -> HandlerOutcome {
    tracing::info!(job.id, "thumbnail made");
    HandlerOutcome::ack()
}
// --8<-- [end:simple]

// --8<-- [start:reliable]
/// The entry waits on a processing list while the handler runs: an ack removes it, a retry puts
/// it back onto `renders`, behind the jobs waiting there.
#[subscriber(RedisList::new("renders").reliable())]
async fn render(job: &Job, State(farm): State<RenderFarm>) -> HandlerOutcome {
    match farm.render(job).await {
        Ok(()) => HandlerOutcome::ack(),
        Err(err) => {
            tracing::warn!(job.id, %err, "render failed, back on the queue");
            HandlerOutcome::retry()
        }
    }
}
// --8<-- [end:reliable]

// --8<-- [start:recovery]
/// Every claim is recorded in `invoices.claims`. An entry still unsettled after `MIN_IDLE` goes
/// back on `invoices`, so set it above the longest time one invoice takes.
#[subscriber(
    RedisList::new("invoices")
        .reliable()
        .min_idle(MIN_IDLE)
        .recovery_zset("invoices.claims")
)]
async fn send_invoice(job: &Job) -> HandlerOutcome {
    tracing::info!(job.id, "invoice sent");
    HandlerOutcome::ack()
}
// --8<-- [end:recovery]

#[ruststream::app]
fn app() -> impl App<State = AppState> {
    RustStream::new(AppInfo::new("jobs", "0.1.0"))
        .on_startup(async move |()| {
            Ok::<_, Infallible>(AppState {
                farm: RenderFarm::default(),
            })
        })
        .with_broker(RedisBroker::standalone("redis://localhost:6379"), |b| {
            b.include(make_thumbnail);
            b.include(render);
            b.include(send_invoice);
        })
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::{Outcome, TestApp};
    use ruststream_fred::list::prelude::*;

    use super::{Job, app};

    #[tokio::test]
    async fn a_simple_queue_hands_out_its_entries_in_order() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        for id in [1, 2] {
            tb.broker::<RedisBroker>()
                .message(&Job { id })
                .to("thumbnails")
                .publish()
                .await?;
        }

        let redis = tb.broker::<RedisBroker>();
        let made = redis.subscriber("thumbnails");
        assert_eq!(made.received::<Job>(), [Job { id: 1 }, Job { id: 2 }]);
        made.assert_called(2).settled(HandlerOutcome::ack());
        Ok(())
    }

    #[tokio::test]
    async fn a_retried_render_comes_back_from_the_queue() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<RedisBroker>()
            .message(&Job { id: 7 })
            .to("renders")
            .publish()
            .await?;

        let redis = tb.broker::<RedisBroker>();
        let renders = redis.subscriber("renders");
        assert_eq!(renders.outcomes(), [Outcome::Nack, Outcome::Ack]);
        renders.assert_called(2).with(&Job { id: 7 });
        // The retry pushed the same entry back onto the queue, where the next pop took it.
        redis
            .published::<Job>("renders")
            .assert_called(2)
            .with(&Job { id: 7 });
        Ok(())
    }

    #[tokio::test]
    async fn a_recovered_queue_delivers_and_settles() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<RedisBroker>()
            .message(&Job { id: 3 })
            .to("invoices")
            .publish()
            .await?;

        tb.broker::<RedisBroker>()
            .subscriber("invoices")
            .assert_called_once()
            .with(&Job { id: 3 })
            .settled(HandlerOutcome::ack());
        Ok(())
    }

    #[subscriber(RedisList::new("invoices").recovery_zset("invoices.claims"))]
    async fn send_invoice_unbounded(job: &Job) -> HandlerOutcome {
        tracing::info!(job.id, "invoice sent");
        HandlerOutcome::ack()
    }

    #[tokio::test]
    async fn a_recovery_set_without_min_idle_does_not_start() {
        let unbounded = RustStream::new(AppInfo::new("jobs", "0.1.0")).with_broker(
            RedisBroker::standalone("redis://localhost:6379"),
            |b| {
                b.include(send_invoice_unbounded);
            },
        );

        let refused = TestApp::start(unbounded)
            .await
            .expect_err("the watchdog needs to know when a claim is stranded");
        assert!(
            refused.to_string().contains("needs a min_idle"),
            "{refused}"
        );
    }
}
