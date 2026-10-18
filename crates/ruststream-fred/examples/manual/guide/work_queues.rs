//! The service of the Work queues on lists guide, written without the macros: the handlers are
//! types with an `impl Handle`, mounted with the `subscriber` constructor, and `main` is written by
//! hand.
//!
//! ```text
//! docker run -p 6379:6379 redis:8.4
//! cargo run --example manual_guide_work_queues --features testing -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;
use std::time::Duration;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_fred::prelude::*;
use serde::{Deserialize, Serialize};

/// Declares what `#[derive(Outgoing)]` declares: the destination form and no header contract.
macro_rules! outgoing {
    ($ty:ty) => {
        impl OutgoingDestination for $ty {
            type Form = CallerName;
        }

        impl MessageHeaders for $ty {
            type Contract = NoHeaders;
        }
    };
    ($ty:ty, $name:literal) => {
        impl OutgoingDestination for $ty {
            type Form = FixedName;
            const DESTINATION: &'static str = $name;
        }

        impl MessageHeaders for $ty {
            type Contract = NoHeaders;
        }
    };
}

// --8<-- [start:simple]
#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Warmup {
    page: String,
}

outgoing!(Warmup, "cache.warmups");

struct WarmCache;

impl Handle<Warmup> for WarmCache {
    fn handle(
        &self,
        warmup: &Warmup,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(warmup.page, "page cached");
        ready(Ok(()))
    }
}
// --8<-- [end:simple]

// --8<-- [start:reliable]
/// Longer than the slowest render ever takes: a claim idle this long belongs to no live worker.
const MIN_IDLE: Duration = Duration::from_secs(60);

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct ThumbnailJob {
    image: String,
}

outgoing!(ThumbnailJob);

struct Render;

impl Handle<ThumbnailJob> for Render {
    fn handle(
        &self,
        job: &ThumbnailJob,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(job.image, "thumbnail rendered");
        ready(Ok(()))
    }
}

fn thumbnails() -> RedisList {
    RedisList::new("{thumbnails}")
        .reliable()
        .min_idle(MIN_IDLE)
        .recovery_zset("{thumbnails}.claims")
}
// --8<-- [end:reliable]

// --8<-- [start:producer]
#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Upload {
    image: String,
}

outgoing!(Upload, "uploads");

struct QueueThumbnail;

impl Handle<Upload, ThumbnailJob> for QueueThumbnail {
    fn handle(
        &self,
        upload: &Upload,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<ThumbnailJob, HandlerOutcome>> {
        ready(Ok(ThumbnailJob {
            image: upload.image.clone(),
        }))
    }
}
// --8<-- [end:producer]

// --8<-- [start:app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("images", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            b.include(subscriber(RedisList::new("cache.warmups"), WarmCache).build());
            b.include(subscriber(thumbnails(), Render).build());
            // A queue nobody fed for a day expires, with whatever is left in it.
            b.include(
                subscriber(
                    RedisStream::new("uploads").group("thumbnailer"),
                    QueueThumbnail,
                )
                .reply()
                .to("{thumbnails}")
                .build(),
            )
            .out_reply(list::Publish::new().ttl(Duration::from_hours(24)));
        },
    )
}

fn main() -> ExitCode {
    run_main(app)
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_fred::prelude::*;

    use super::{MIN_IDLE, ThumbnailJob, Upload, Warmup, app};

    #[tokio::test]
    async fn a_warmup_is_handled_once() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let warmup = Warmup {
            page: "/pricing".to_owned(),
        };

        tb.broker::<RedisBroker>()
            .message(&warmup)
            .publish()
            .await?;

        tb.broker::<RedisBroker>()
            .subscriber("cache.warmups")
            .assert_called_once()
            .with(&warmup)
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn an_upload_queues_a_job_that_is_rendered() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<RedisBroker>()
            .message(&Upload {
                image: "cat.png".to_owned(),
            })
            .publish()
            .await?;

        let job = ThumbnailJob {
            image: "cat.png".to_owned(),
        };
        tb.broker::<RedisBroker>()
            .published::<ThumbnailJob>("{thumbnails}")
            .assert_called_once()
            .with(&job);
        tb.broker::<RedisBroker>()
            .subscriber("{thumbnails}")
            .assert_called_once()
            .with(&job)
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_rendered_job_is_not_recovered() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<RedisBroker>()
            .message(&ThumbnailJob {
                image: "dog.png".to_owned(),
            })
            .to("{thumbnails}")
            .publish()
            .await?;

        // The acknowledgement removed the claim, so the watchdog has nothing to return.
        tb.advance(MIN_IDLE * 2).await?;
        tb.broker::<RedisBroker>()
            .subscriber("{thumbnails}")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
}
