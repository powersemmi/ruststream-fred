//! The service of `examples/redis_lists.rs` written without the macros: each handler implements
//! `Handle`, and `subscriber(descriptor, handler)` mounts it on its list.
//!
//! ```text
//! cargo test --example manual_redis_lists --features testing
//! cargo run --example manual_redis_lists --features testing -- run
//! ```

use std::convert::Infallible;
use std::future::{Future, ready};
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::{self, JsonSchema};
use ruststream_fred::list::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
struct Job {
    id: u64,
}

// A stand-in for the render farm the jobs go to: it turns away the first job it is sent while it
// warms up, and takes every job after that. Wired once at startup, read through `Context::state`.
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

/// What the handlers share, built once at startup.
struct AppState {
    farm: RenderFarm,
}

/// How long a claimed invoice may sit unsettled before the watchdog returns it to the queue.
const MIN_IDLE: Duration = Duration::from_secs(60);

// --8<-- [start:simple]
/// Each entry of `thumbnails` reaches one consumer. It left Redis when it was popped, so the
/// outcome settles nothing.
struct MakeThumbnail;

impl Handle<Job, (), (), (), AppState> for MakeThumbnail {
    fn handle(
        &self,
        job: &Job,
        _outs: &(),
        _ctx: &mut Context<'_, (), AppState>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(job.id, "thumbnail made");
        ready(Ok(()))
    }
}

fn thumbnails() -> RedisList {
    RedisList::new("thumbnails")
}
// --8<-- [end:simple]

// --8<-- [start:reliable]
/// The entry waits on a processing list while the handler runs: an ack removes it, a retry puts
/// it back on `renders` for the next pop.
struct Render;

impl Handle<Job, (), (), (), AppState> for Render {
    async fn handle(
        &self,
        job: &Job,
        _outs: &(),
        ctx: &mut Context<'_, (), AppState>,
    ) -> Result<(), HandlerOutcome> {
        if let Err(err) = ctx.state().farm.render(job).await {
            tracing::warn!(job.id, %err, "render failed, back on the queue");
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}

fn renders() -> RedisList {
    RedisList::new("renders").reliable()
}
// --8<-- [end:reliable]

// --8<-- [start:recovery]
/// Every claim is recorded in `invoices.claims`. An entry still unsettled after `MIN_IDLE` goes
/// back on `invoices`, so set it above the longest time one invoice takes.
struct SendInvoice;

impl Handle<Job, (), (), (), AppState> for SendInvoice {
    fn handle(
        &self,
        job: &Job,
        _outs: &(),
        _ctx: &mut Context<'_, (), AppState>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(job.id, "invoice sent");
        ready(Ok(()))
    }
}

fn invoices() -> RedisList {
    RedisList::new("invoices")
        .reliable()
        .min_idle(MIN_IDLE)
        .recovery_zset("invoices.claims")
}
// --8<-- [end:recovery]

fn app() -> impl App<State = AppState> {
    RustStream::new(AppInfo::new("jobs", "0.1.0"))
        .on_startup(async move |()| {
            Ok::<_, Infallible>(AppState {
                farm: RenderFarm::default(),
            })
        })
        .with_broker(RedisBroker::standalone("redis://localhost:6379"), |b| {
            b.include(subscriber(thumbnails(), MakeThumbnail).build());
            b.include(subscriber(renders(), Render).build());
            b.include(subscriber(invoices(), SendInvoice).build());
        })
}

fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::error::Error;

    use ruststream::testing::{Outcome, TestApp};
    use ruststream_fred::list::prelude::*;

    use super::{AppState, Job, RenderFarm, SendInvoice, app};

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

    #[tokio::test]
    async fn a_recovery_set_without_min_idle_does_not_start() {
        let unbounded = RustStream::new(AppInfo::new("jobs", "0.1.0"))
            .on_startup(async move |()| {
                Ok::<_, Infallible>(AppState {
                    farm: RenderFarm::default(),
                })
            })
            .with_broker(RedisBroker::standalone("redis://localhost:6379"), |b| {
                let descriptor = RedisList::new("invoices").recovery_zset("invoices.claims");
                b.include(subscriber(descriptor, SendInvoice).build());
            });

        let refused = TestApp::start(unbounded)
            .await
            .expect_err("the watchdog needs to know when a claim is stranded");
        assert!(
            refused.to_string().contains("needs a min_idle"),
            "{refused}"
        );
    }
}
