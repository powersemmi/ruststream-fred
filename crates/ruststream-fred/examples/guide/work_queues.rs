//! The service of the Work queues on lists guide: an image service that renders thumbnails off a
//! reliable Redis list and warms its page cache off a simple one.
//!
//! ```text
//! docker run -p 6379:6379 redis:8.4
//! cargo run --example guide_work_queues --features testing -- run
//! ```

use std::time::Duration;

use ruststream_fred::prelude::*;
use serde::{Deserialize, Serialize};

// --8<-- [start:simple]
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "cache.warmups")]
struct Warmup {
    page: String,
}

#[subscriber(RedisList::new("cache.warmups"))]
async fn warm_cache(warmup: &Warmup) -> HandlerOutcome {
    tracing::info!(warmup.page, "page cached");
    HandlerOutcome::ack()
}
// --8<-- [end:simple]

// --8<-- [start:reliable]
/// Longer than the slowest render ever takes: a claim idle this long belongs to no live worker.
const MIN_IDLE: Duration = Duration::from_secs(60);

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct ThumbnailJob {
    image: String,
}

#[subscriber(
    RedisList::new("{thumbnails}")
        .reliable()
        .min_idle(MIN_IDLE)
        .recovery_zset("{thumbnails}.claims")
)]
async fn render(job: &ThumbnailJob) -> HandlerOutcome {
    tracing::info!(job.image, "thumbnail rendered");
    HandlerOutcome::ack()
}
// --8<-- [end:reliable]

// --8<-- [start:producer]
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "uploads")]
struct Upload {
    image: String,
}

// Braces in `#[outgoing(name = ..)]` read as a name template, so the mount site names the key.
#[subscriber(RedisStream::new("uploads").group("thumbnailer"), reply("{thumbnails}"))]
async fn queue_thumbnail(upload: &Upload) -> ThumbnailJob {
    ThumbnailJob {
        image: upload.image.clone(),
    }
}
// --8<-- [end:producer]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("images", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            b.include(warm_cache);
            b.include(render);
            // A queue nobody fed for a day expires, with whatever is left in it.
            b.include(queue_thumbnail)
                .out_reply(list::Publish::new().ttl(Duration::from_hours(24)));
        },
    )
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
