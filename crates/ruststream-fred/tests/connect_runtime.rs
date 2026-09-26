//! The broker's own tasks run on the runtime it connected on, whichever runtime the call that
//! starts them comes from.
//!
//! A handler on a dedicated thread publishes, settles and drops deliveries from that thread's
//! current-thread runtime, and a caller may open a subscription from one. Each case here makes
//! such a call from a runtime of its own on another thread, stops that runtime, and checks the work
//! the call left behind still reaches the server. The stream cases run in process; the Pub/Sub
//! connection case needs a live server, because the in-process mode delivers Pub/Sub messages past
//! the client's connection task.
//!
//! ```bash
//! just test-brokers
//! ```

#![cfg(feature = "testing")]

use std::pin::pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fred::interfaces::StreamsInterface;
use futures::{Stream, StreamExt};
use ruststream::testing::InProcess;
use ruststream::{
    Broker, ConnectedBroker, IncomingMessage, OutgoingMessage, Publisher, Subscriber,
    SubscriptionSource,
};
use ruststream_fred::{
    ConnectedRedisBroker, PipelinedStream, RedisBroker, RedisError, RedisPubSub,
    RedisPubSubPublish, RedisStream,
};
use tokio::runtime::Builder;
use tokio::sync::oneshot;
use tokio::time::{Instant, timeout};

mod live;

/// The address the service's broker is built with; the in-process mode dials nothing.
const URL: &str = "redis://localhost:6379";

const WAIT: Duration = Duration::from_secs(5);

/// How long an entry sits pending before a claiming subscription takes it over.
const MIN_IDLE: Duration = Duration::from_millis(200);

/// A name unique to this run, so a live server that keeps an earlier run's keys or subscriptions
/// answers nothing of this one.
fn unique(base: &str) -> String {
    static RUN: OnceLock<u128> = OnceLock::new();
    static N: AtomicU64 = AtomicU64::new(0);
    let run = RUN.get_or_init(|| {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("a clock after the epoch")
            .as_nanos()
    });
    format!(
        "ruststream-rt.{base}.{run}.{}",
        N.fetch_add(1, Ordering::Relaxed)
    )
}

/// Runs `work` on a current-thread runtime of its own on another thread, and stops that runtime
/// as soon as `work` returns: a task the work spawned there dies with it.
async fn on_foreign_runtime<Work, Fut, Output>(work: Work) -> Output
where
    Work: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = Output>,
    Output: Send + 'static,
{
    let (done, result) = oneshot::channel();
    thread::spawn(move || {
        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a current-thread runtime builds");
        let output = runtime.block_on(work());
        drop(runtime);
        let _ = done.send(output);
    });
    result.await.expect("the foreign runtime's work panicked")
}

/// Runs `work` on another thread with no runtime at all, the way a destructor runs when the last
/// owner is dropped outside any runtime.
async fn off_any_runtime(work: impl FnOnce() + Send + 'static) {
    let (done, finished) = oneshot::channel();
    thread::spawn(move || {
        work();
        let _ = done.send(());
    });
    finished
        .await
        .expect("the work outside any runtime panicked");
}

async fn in_process() -> ConnectedRedisBroker {
    RedisBroker::standalone(URL)
        .connect_in_process()
        .await
        .expect("connect in process")
}

async fn next<S, M>(stream: &mut S) -> M
where
    S: Stream<Item = Result<M, RedisError>> + Unpin,
{
    timeout(WAIT, stream.next())
        .await
        .expect("a delivery within the wait")
        .expect("the stream is open")
        .expect("a delivery, not an error")
}

async fn publish_entries(broker: &ConnectedRedisBroker, key: &str, count: usize) {
    let publisher = broker.publisher();
    for _ in 0..count {
        publisher
            .publish(OutgoingMessage::new(key, b"entry".as_slice()), None)
            .await
            .expect("publish");
    }
}

/// Waits until the group owes exactly `expected` entries, and fails once the wait runs out.
async fn assert_pending(broker: &ConnectedRedisBroker, key: &str, expected: u64, why: &str) {
    let pool = broker.pool_handle().expect("live pool");
    let deadline = Instant::now() + WAIT;
    loop {
        let rows: Vec<(String, String, u64, u64)> = pool
            .xpending(key, "workers", (0_u64, "-", "+", 10_u64))
            .await
            .expect("xpending");
        if rows.len() as u64 == expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{why}: the group still owes {} entries, expected {expected}",
            rows.len()
        );
        tokio::task::yield_now().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_delivery_dropped_on_another_runtime_flushes_what_its_window_owes() {
    let broker = in_process().await;
    let key = unique("abandon");
    let mut subscriber = PipelinedStream::new(key.as_str())
        .group("workers")
        .subscribe(&broker)
        .await
        .expect("subscribe");
    publish_entries(&broker, &key, 2).await;
    let (acked, dropped) = {
        let mut stream = pin!(subscriber.stream());
        (next(&mut stream).await, next(&mut stream).await)
    };

    // The ack waits in the window for the other delivery; dropping that one unsettled is what
    // sends the window, from a runtime that stops right after.
    on_foreign_runtime(async move || {
        acked.ack().await.expect("ack");
        drop(dropped);
    })
    .await;

    assert_pending(
        &broker,
        &key,
        1,
        "the window flush left on a runtime that stopped",
    )
    .await;
    drop(subscriber);
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_subscription_dropped_off_any_runtime_flushes_what_its_window_owes() {
    let broker = in_process().await;
    let key = unique("stop");
    let mut subscriber = PipelinedStream::new(key.as_str())
        .group("workers")
        .subscribe(&broker)
        .await
        .expect("subscribe");
    publish_entries(&broker, &key, 2).await;
    let (acked, held) = {
        let mut stream = pin!(subscriber.stream());
        (next(&mut stream).await, next(&mut stream).await)
    };
    acked.ack().await.expect("ack");

    // The subscription stops while a delivery is still in hand: its drop owes the server the ack
    // the window holds, and it happens where no runtime is current.
    off_any_runtime(move || drop(subscriber)).await;

    assert_pending(&broker, &key, 1, "the stopping subscription sent nothing").await;
    drop(held);
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_claiming_subscription_opened_on_another_runtime_claims_what_comes_due() {
    let broker = Arc::new(in_process().await);
    let key = unique("claim");
    let mut first = broker
        .subscribe(
            RedisStream::new(key.as_str())
                .group("workers")
                .consumer("first"),
        )
        .await
        .expect("subscribe");
    publish_entries(&broker, &key, 1).await;
    {
        let mut stream = pin!(first.stream());
        // Read and dropped unsettled: the entry stays pending with the first consumer.
        drop(next(&mut stream).await);
    }

    // Opening the claiming subscription arms the timer that makes the pending entry claimable,
    // from a runtime that stops once the subscription is open.
    let opener = Arc::clone(&broker);
    let def = RedisStream::claiming(key.as_str(), MIN_IDLE)
        .group("workers")
        .consumer("second");
    let mut second =
        on_foreign_runtime(async move || opener.subscribe(def).await.expect("subscribe")).await;

    let claimed = {
        let mut stream = pin!(second.stream());
        next(&mut stream).await
    };
    assert_eq!(claimed.payload(), b"entry");
    claimed.ack().await.expect("ack");
    drop((first, second));
    let broker = Arc::into_inner(broker).expect("the opener let go of the broker");
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pubsub_subscription_dropped_off_any_runtime_closes_its_connection() {
    let broker = in_process().await;
    let subscriber = broker
        .subscribe_pubsub(RedisPubSub::new(unique("drop")))
        .await
        .expect("subscribe");

    // The dedicated connection is closed from its subscription's destructor, which has to reach
    // the broker's runtime rather than whatever runtime is current where the drop happens.
    off_any_runtime(move || drop(subscriber)).await;

    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pubsub_subscription_opened_on_another_runtime_keeps_its_connection() {
    let Some(url) = live::url("REDIS_TEST_URL") else {
        return;
    };
    let broker = RedisBroker::standalone(url)
        .connect()
        .await
        .expect("connect");
    let channel = unique("open");

    // The dedicated connection is dialled from a runtime that stops once the subscription is open;
    // the task driving the connection has to live on the broker's runtime.
    let broker = Arc::new(broker);
    let opener = Arc::clone(&broker);
    let def = RedisPubSub::new(channel.as_str());
    let mut subscriber =
        on_foreign_runtime(async move || opener.subscribe_pubsub(def).await.expect("subscribe"))
            .await;

    broker
        .pubsub_publisher(RedisPubSubPublish::new())
        .publish(OutgoingMessage::new(&channel, b"after".as_slice()), None)
        .await
        .expect("publish");
    let message = {
        let mut stream = pin!(subscriber.stream());
        next(&mut stream).await
    };
    assert_eq!(message.payload(), b"after");
    drop(subscriber);
    let broker = Arc::into_inner(broker).expect("the opener let go of the broker");
    broker.shutdown().await.expect("shutdown");
}
