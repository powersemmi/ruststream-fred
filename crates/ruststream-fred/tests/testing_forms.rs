//! The in-process server keeps Redis's three namespaces apart, the way a server does.
//!
//! A stream and a list are keys of a type: `XADD` on a list, or `LPUSH` on a stream, is a
//! `WRONGTYPE` error, and a read of the other type fails the same way. A Pub/Sub channel is no key
//! at all: a stream or a list written under a channel's name is a key its subscribers never see,
//! and a `PUBLISH` reaches no stream or list. The server takes both and delivers nothing, and so
//! does the in-process server.

#![cfg(feature = "testing")]

use std::time::Duration;

use futures::StreamExt;
use futures::future::BoxFuture;
use ruststream::testing::InProcess;
use ruststream::{
    IncomingMessage, OutgoingMessage, PublishPolicy, Publisher, Subscriber, SubscriptionSource,
};
use ruststream_fred::{
    ConnectedRedisBroker, RedisBroker, RedisList, RedisListPublish, RedisPubSub,
    RedisPubSubPublish, RedisStream,
};

/// The address the service's broker is built with; the in-process mode dials nothing.
const URL: &str = "redis://localhost:6379";

const WAIT: Duration = Duration::from_secs(1);

/// How long a case waits to be sure nothing arrives.
const QUIET: Duration = Duration::from_millis(50);

async fn connected() -> ConnectedRedisBroker {
    RedisBroker::standalone(URL)
        .connect_in_process()
        .await
        .expect("connect")
}

/// A key nobody wrote yet takes the type of its first write: an `XADD` to the name a list reads
/// makes it a stream, and the list's next pop fails as it fails on a server.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stream_publish_to_a_key_a_list_reads_breaks_the_list_read() {
    let broker = connected().await;
    let mut jobs = RedisList::new("jobs")
        .reliable()
        .subscribe(&broker)
        .await
        .expect("subscribe");

    broker
        .publisher()
        .publish(OutgoingMessage::new("jobs", b"x"), None)
        .await
        .expect("XADD creates a stream under a name nothing holds yet");

    let mut stream = Box::pin(jobs.stream());
    let err = tokio::time::timeout(WAIT, stream.next())
        .await
        .expect("the read answers within the wait")
        .expect("the stream has a next item")
        .expect_err("a pop on a stream key fails");
    assert!(err.to_string().contains("WRONGTYPE"), "got {err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_list_publish_to_a_key_a_stream_reads_is_refused() {
    let broker = connected().await;
    let _orders = RedisStream::new("orders")
        .group("workers")
        .subscribe(&broker)
        .await
        .expect("subscribe");

    let lists = RedisListPublish::new().pair(&broker).await.expect("pair");
    let err = lists
        .publish(OutgoingMessage::new("orders", b"x"), None)
        .await
        .expect_err("LPUSH on a stream key is WRONGTYPE on a server");
    assert!(err.to_string().contains("WRONGTYPE"), "got {err}");
}

/// A channel is no key: an `XADD` under its name writes a stream its subscribers never see.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stream_publish_to_a_name_a_channel_reads_reaches_no_subscriber() {
    let broker = connected().await;
    let mut events = RedisPubSub::new("events")
        .subscribe(&broker)
        .await
        .expect("subscribe");

    broker
        .publisher()
        .publish(OutgoingMessage::new("events", b"x"), None)
        .await
        .expect("an XADD under a channel's name is a stream write");

    let mut stream = Box::pin(events.stream());
    assert!(
        tokio::time::timeout(QUIET, stream.next()).await.is_err(),
        "a stream write is not a channel message"
    );
}

/// A `PUBLISH` reaches subscribers only: the list under the same name stays empty.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_channel_publish_to_a_key_a_list_reads_reaches_no_list() {
    let broker = connected().await;
    let mut jobs = RedisList::new("jobs")
        .subscribe(&broker)
        .await
        .expect("subscribe");

    let channels = RedisPubSubPublish::new().pair(&broker).await.expect("pair");
    channels
        .publish(OutgoingMessage::new("jobs", b"x"), None)
        .await
        .expect("a PUBLISH with no subscriber is taken and delivered to nobody");

    let mut stream = Box::pin(jobs.stream());
    assert!(
        tokio::time::timeout(QUIET, stream.next()).await.is_err(),
        "a channel message is not a list element"
    );
}

/// A key a stream publish created is a stream from then on, with nobody subscribed to it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_key_keeps_the_type_its_first_write_gave_it() {
    let broker = connected().await;
    broker
        .publisher()
        .publish(OutgoingMessage::new("audit", b"x"), None)
        .await
        .expect("XADD creates the stream");

    let lists = RedisListPublish::new().pair(&broker).await.expect("pair");
    let err = lists
        .publish(OutgoingMessage::new("audit", b"y"), None)
        .await
        .expect_err("LPUSH on the stream the XADD created is WRONGTYPE");
    assert!(err.to_string().contains("WRONGTYPE"), "got {err}");
}

/// Each form's own publish reaches the subscription of that form.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_form_is_reached_by_its_own_publish() {
    let broker = connected().await;
    let mut orders = RedisStream::new("orders")
        .group("workers")
        .subscribe(&broker)
        .await
        .expect("subscribe stream");
    let mut jobs = RedisList::new("jobs")
        .reliable()
        .subscribe(&broker)
        .await
        .expect("subscribe list");
    let mut events = RedisPubSub::new("events")
        .subscribe(&broker)
        .await
        .expect("subscribe channel");

    broker
        .publisher()
        .publish(OutgoingMessage::new("orders", b"o"), None)
        .await
        .expect("XADD");
    RedisListPublish::new()
        .pair(&broker)
        .await
        .expect("pair")
        .publish(OutgoingMessage::new("jobs", b"j"), None)
        .await
        .expect("LPUSH");
    RedisPubSubPublish::new()
        .pair(&broker)
        .await
        .expect("pair")
        .publish(OutgoingMessage::new("events", b"e"), None)
        .await
        .expect("PUBLISH");

    for (subscription, expected) in [
        (&mut orders as &mut dyn NextPayload, b"o"),
        (&mut jobs, b"j"),
        (&mut events, b"e"),
    ] {
        assert_eq!(subscription.next_payload().await, expected);
    }
}

/// Reads one payload off a subscription of any form, so one loop checks all three.
trait NextPayload {
    fn next_payload(&mut self) -> BoxFuture<'_, Vec<u8>>;
}

impl<S> NextPayload for S
where
    S: Subscriber + Send,
    S::Message: Send,
{
    fn next_payload(&mut self) -> BoxFuture<'_, Vec<u8>> {
        Box::pin(async move {
            let mut stream = Box::pin(self.stream());
            let delivered = tokio::time::timeout(WAIT, stream.next())
                .await
                .expect("a delivery within the wait")
                .expect("the stream has a next item")
                .unwrap_or_else(|_| panic!("the delivery failed"));
            delivered.payload().to_vec()
        })
    }
}
