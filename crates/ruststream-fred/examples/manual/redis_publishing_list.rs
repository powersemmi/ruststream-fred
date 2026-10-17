//! The service of `examples/redis_publishing_list.rs` written without `#[subscriber]`.
//!
//! ```text
//! cargo test --example manual_redis_publishing_list --features testing
//! cargo run --example manual_redis_publishing_list -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;
use std::time::Duration;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_fred::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "label-jobs")]
struct LabelJob {
    order_id: u64,
}

// --8<-- [start:list_publish]
struct RequestLabel;

impl Handle<Order, LabelJob> for RequestLabel {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<LabelJob, HandlerOutcome>> {
        ready(Ok(LabelJob { order_id: order.id }))
    }
}

fn app() -> impl App {
    // A queue nobody drains for an hour expires instead of growing.
    let jobs = list::Publish::new().ttl(Duration::from_secs(3600));
    RustStream::new(AppInfo::new("labels", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            let request = subscriber(RedisStream::new("orders").group("labels"), RequestLabel);
            b.include(request.reply().build()).out_reply(jobs);
        },
    )
}
// --8<-- [end:list_publish]

fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_fred::prelude::*;

    use super::{LabelJob, Order, app};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_order_pushes_its_label_job() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<RedisBroker>()
            .message(&Order { id: 31 })
            .publish()
            .await?;

        tb.broker::<RedisBroker>()
            .published::<LabelJob>("label-jobs")
            .assert_called_once()
            .with(&LabelJob { order_id: 31 });
        tb.shutdown().await?;
        Ok(())
    }
}
