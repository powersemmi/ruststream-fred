//! `RedisListPublish`, `Publish` in the list prelude: a reply pushed onto a list with `LPUSH`,
//! with the list's expiry armed again at every push.
//!
//! ```text
//! cargo test --example redis_publishing_list --features testing
//! cargo run --example redis_publishing_list -- run
//! ```

use std::time::Duration;

use ruststream_fred::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "label-jobs")]
struct LabelJob {
    order_id: u64,
}

// --8<-- [start:list_publish]
#[subscriber(RedisStream::new("orders").group("labels"), reply)]
async fn request_label(order: &Order) -> LabelJob {
    LabelJob { order_id: order.id }
}

#[ruststream::app]
fn app() -> impl App {
    // A queue nobody drains for an hour expires instead of growing.
    let jobs = list::Publish::new().ttl(Duration::from_secs(3600));
    RustStream::new(AppInfo::new("labels", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            b.include(request_label).out_reply(jobs);
        },
    )
}
// --8<-- [end:list_publish]

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
