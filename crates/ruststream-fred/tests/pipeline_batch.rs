//! A batch mount on a `.pipeline()` subscription: the batch is one segment of the window, so what
//! the batch body queued leaves with the settles of every entry, and only if every entry is
//! acknowledged.

#![cfg(feature = "testing")]

use ruststream::testing::TestApp;
use ruststream_fred::PipelinedStream;
use ruststream_fred::context::{PipelineContext, keys};
use ruststream_fred::prelude::*;
use ruststream_fred::testing::RedisTestBroker;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Outgoing, PartialEq, Serialize)]
struct Order {
    id: u64,
    keep: bool,
}

#[subscriber(PipelinedStream::new("orders").group("workers"))]
async fn in_batches(orders: &[Order], ctx: &mut Context<'_, PipelineContext>) -> HandlerOutcome {
    let pipeline = ctx.context(keys::Pipeline).clone();
    for order in orders {
        let body = serde_json::to_vec(order).expect("an order encodes");
        if pipeline.lpush("audit", body).await.is_err() {
            return HandlerOutcome::retry();
        }
    }
    if orders.iter().all(|order| order.keep) {
        HandlerOutcome::ack()
    } else {
        HandlerOutcome::drop()
    }
}

macro_rules! case {
    ($test:ident, $keep:literal, $expected:literal) => {
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn $test() {
            let app = RustStream::new(AppInfo::new("pipeline", "0.1.0")).with_broker(
                RedisTestBroker::new(),
                |b| {
                    b.include(in_batches.batch(nonzero!(3)));
                },
            );
            let tb = TestApp::start(app).await.expect("start");

            for id in 0..3 {
                tb.broker::<RedisTestBroker>()
                    .message(&Order { id, keep: $keep })
                    .to("orders")
                    .publish()
                    .await
                    .expect("publish");
            }

            tb.broker::<RedisTestBroker>()
                .published::<Order>("audit")
                .assert_called($expected);

            tb.shutdown().await.expect("shutdown");
        }
    };
}

case!(an_acknowledged_batch_sends_what_it_queued, true, 3);
case!(a_dropped_batch_sends_nothing_it_queued, false, 0);
