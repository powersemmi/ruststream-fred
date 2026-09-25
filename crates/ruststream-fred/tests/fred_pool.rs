//! `Ctx<keys::FredPool>` hands a handler the broker's connection pool on every subscription form,
//! for a command whose answer the handler needs now. The in-process broker hands out a pool whose
//! writes it applies as a server would, so a handler's own `LPUSH` is observable in the harness.

#![cfg(feature = "testing")]

use fred::clients::Pool;
use fred::interfaces::ListInterface;
use ruststream::testing::TestApp;
use ruststream_fred::context::keys;
use ruststream_fred::prelude::*;
use serde::{Deserialize, Serialize};

/// The address the service's broker is built with; the in-process mode dials nothing.
const URL: &str = "redis://localhost:6379";

#[derive(Debug, Deserialize, Outgoing, PartialEq, Serialize)]
struct Order {
    id: u64,
}

/// Writes the order to the audit list through the pool, and settles by what the write returned.
async fn audit(pool: &Pool, order: &Order) -> HandlerOutcome {
    let body = format!(r#"{{"id":{}}}"#, order.id);
    match pool.lpush::<i64, _, _>("audit", body).await {
        Ok(_) => HandlerOutcome::ack(),
        Err(_) => HandlerOutcome::retry(),
    }
}

#[subscriber(RedisStream::new("orders").group("workers"))]
async fn from_stream(order: &Order, Ctx(pool): Ctx<keys::FredPool>) -> HandlerOutcome {
    audit(&pool, order).await
}

#[subscriber(RedisList::new("jobs").reliable())]
async fn from_reliable_list(order: &Order, Ctx(pool): Ctx<keys::FredPool>) -> HandlerOutcome {
    audit(&pool, order).await
}

#[subscriber(RedisList::new("tasks"))]
async fn from_plain_list(order: &Order, Ctx(pool): Ctx<keys::FredPool>) -> HandlerOutcome {
    audit(&pool, order).await
}

#[subscriber(RedisPubSub::new("events"))]
async fn from_channel(order: &Order, Ctx(pool): Ctx<keys::FredPool>) -> HandlerOutcome {
    audit(&pool, order).await
}

/// Mounts `handler` alone, delivers one order on `name`, and checks the handler's own write.
macro_rules! writes_through_the_pool {
    ($test:ident, $handler:ident, $name:literal) => {
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn $test() {
            let app = RustStream::new(AppInfo::new("pool", "0.1.0")).with_broker(
                RedisBroker::standalone(URL),
                |b| {
                    b.include($handler);
                },
            );
            let tb = TestApp::start(app).await.expect("start");

            tb.broker::<RedisBroker>()
                .message(&Order { id: 5 })
                .to($name)
                .publish()
                .await
                .expect("publish");

            tb.broker::<RedisBroker>()
                .subscriber($name)
                .assert_called_once()
                .settled(HandlerOutcome::ack());
            tb.broker::<RedisBroker>()
                .published::<Order>("audit")
                .assert_called_once()
                .with(&Order { id: 5 });

            tb.shutdown().await.expect("shutdown");
        }
    };
}

writes_through_the_pool!(
    a_stream_handler_writes_through_the_pool,
    from_stream,
    "orders"
);
writes_through_the_pool!(
    a_reliable_list_handler_writes_through_the_pool,
    from_reliable_list,
    "jobs"
);
writes_through_the_pool!(
    a_plain_list_handler_writes_through_the_pool,
    from_plain_list,
    "tasks"
);
writes_through_the_pool!(
    a_channel_handler_writes_through_the_pool,
    from_channel,
    "events"
);

#[subscriber(RedisStream::new("orders").group("workers"))]
async fn with_entry_id(
    order: &Order,
    Ctx(entry): Ctx<keys::EntryId>,
    Ctx(pool): Ctx<keys::FredPool>,
) -> HandlerOutcome {
    let _ = (order, entry, pool);
    HandlerOutcome::ack()
}

#[subscriber(RedisPubSub::new("events"))]
async fn with_channel(
    order: &Order,
    Ctx(channel): Ctx<keys::Channel>,
    Ctx(pool): Ctx<keys::FredPool>,
) -> HandlerOutcome {
    let _ = (order, channel, pool);
    HandlerOutcome::ack()
}

/// The pool rides beside a form's own keys, so a handler reads both from one context.
#[test]
fn the_pool_rides_beside_a_forms_own_keys() {
    let app = RustStream::new(AppInfo::new("pool", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            b.include(with_entry_id);
            b.include(with_channel);
        },
    );
    let _ = app;
}
