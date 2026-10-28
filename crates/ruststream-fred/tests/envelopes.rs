//! How a message's headers travel beside its payload, per envelope, seen from a service: each
//! built-in envelope carries a message round on a list, a channel and a stream; faststream's frame
//! is read and written byte for byte; a value that is no envelope arrives as its payload; `NoOp`
//! refuses a header instead of losing it; and a subscription that never reads headers never
//! parses them.

#![cfg(feature = "testing")]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use fred::interfaces::{ListInterface, PubsubInterface, StreamsInterface};
use ruststream::codec::JsonCodec;
use ruststream::runtime::{App, AppInfo, Ctx, HandlerOutcome, Headers, Out, RustStream};
use ruststream::testing::TestApp;
use ruststream::{Deserialized, HeaderMap, OutSlot, Outgoing, Publisher, Serialized, subscriber};
use ruststream_fred::context::keys;
use ruststream_fred::envelope::{self, Envelope, Location, SealError};
use ruststream_fred::pipeline::RedisPipelineSteps;
use ruststream_fred::{
    FastStream, Framed, NoOp, RedisBroker, RedisEnvelopeExt, RedisList, RedisListPublish,
    RedisPubSub, RedisPublishOptions, RedisPublishSteps, RedisStream,
};
use serde::{Deserialize, Serialize};

const URL: &str = "redis://localhost:6379";

/// The payload every case sends.
const ORDER: &[u8] = br#"{"id":7}"#;

/// What faststream 0.7.8 writes for `ORDER` with the header `tenant: acme` and the correlation id
/// `c-1`: `BinaryMessageFormatV1.encode(message=b'{"id":7}', headers={"tenant": "acme"},
/// correlation_id="c-1", reply_to=None)`.
const FASTSTREAM_FRAME: &[u8] =
    b"\x89BIN\r\n\x1a\n\x00\x01\x00\x00\x00\x12\x00\x00\x00\x37\x00\x02\
    \x00\x0ecorrelation_id\x00\x03c-1\x00\x06tenant\x00\x04acme{\"id\":7}";

/// The delivery's bytes as they arrive.
#[derive(Deserialized)]
struct Raw<'a>(&'a [u8]);

/// Bytes that leave as they are.
#[derive(Outgoing, Serialized)]
struct Payload(Vec<u8>);

/// The header every case carries.
#[derive(Serialize, Deserialize)]
struct Tenant {
    tenant: String,
}

/// Bytes that leave as they are, with the tenant header.
#[derive(Outgoing, Serialized)]
#[outgoing(headers = Tenant)]
struct Tagged(Vec<u8>);

fn acme() -> Tenant {
    Tenant {
        tenant: "acme".to_owned(),
    }
}

/// Acks a delivery that carries the tenant header, drops one that carries none.
fn by_tenant(headers: &BTreeMap<String, String>) -> HandlerOutcome {
    if headers.get("tenant").map(String::as_str) == Some("acme") {
        HandlerOutcome::ack()
    } else {
        HandlerOutcome::drop()
    }
}

#[subscriber(RedisList::new("env.jobs"))]
async fn job(
    _raw: &Raw<'_>,
    Headers(headers): Headers<BTreeMap<String, String>>,
) -> HandlerOutcome {
    by_tenant(&headers)
}

#[subscriber(RedisPubSub::new("env.events"))]
async fn event(
    _raw: &Raw<'_>,
    Headers(headers): Headers<BTreeMap<String, String>>,
) -> HandlerOutcome {
    by_tenant(&headers)
}

#[subscriber(RedisStream::new("env.orders").group("workers"))]
async fn order(
    _raw: &Raw<'_>,
    Headers(headers): Headers<BTreeMap<String, String>>,
) -> HandlerOutcome {
    by_tenant(&headers)
}

/// The names the three forms read, in the order the cases send to them.
const FORMS: [&str; 3] = ["env.jobs", "env.events", "env.orders"];

/// One service on a list, a channel and a stream, its broker reading through `envelope`.
fn app<Env: Envelope>(envelope: Env) -> impl App {
    RustStream::new(AppInfo::new("envelopes", "0.1.0")).with_broker(
        RedisBroker::standalone(URL).envelope(envelope),
        |b| {
            b.include(job);
            b.include(event);
            b.include(order);
        },
    )
}

/// Sends `ORDER` with the tenant header to each form, written in the broker's envelope, and holds
/// each handler to the payload and the header it read back.
async fn round_trip<Env: Envelope>(envelope: Env) {
    let tb = TestApp::start(app(envelope)).await.expect("start");

    for name in FORMS {
        tb.broker::<RedisBroker<Env>>()
            .message(&Tagged(ORDER.to_vec()))
            .to(name)
            .with_headers(&acme())
            .publish()
            .await
            .expect("publish");
        tb.broker::<RedisBroker<Env>>()
            .subscriber(name)
            .assert_called_once()
            .with_raw(ORDER)
            .settled(HandlerOutcome::ack());
    }

    tb.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_default_envelope_carries_a_message_round_on_every_form() {
    round_trip(envelope::RustStream).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn faststream_carries_a_message_round_on_every_form() {
    round_trip(FastStream).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_codec_document_carries_a_message_round_on_every_form() {
    round_trip(Framed::new(JsonCodec)).await;
}

/// `NoOp` carries the payload alone: the value is the message, and the handler sees no headers.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_envelope_carries_the_payload_on_every_form() {
    let tb = TestApp::start(app(NoOp)).await.expect("start");

    for name in FORMS {
        tb.broker::<RedisBroker<NoOp>>()
            .message(&Payload(ORDER.to_vec()))
            .to(name)
            .publish()
            .await
            .expect("publish");
        tb.broker::<RedisBroker<NoOp>>()
            .subscriber(name)
            .assert_called_once()
            .with_raw(ORDER)
            .settled(HandlerOutcome::drop());
    }

    tb.shutdown().await.expect("shutdown");
}

/// Stands in for a faststream service: on any trigger it writes faststream's frame to a list and
/// a channel, and to a stream in its `__data__` field, with the pool's own commands.
#[subscriber(RedisList::new("fs.trigger"))]
async fn faststream_producer(_trigger: &Raw<'_>, Ctx(pool): Ctx<keys::FredPool>) -> HandlerOutcome {
    let pushed: Result<i64, _> = pool.lpush("env.jobs", FASTSTREAM_FRAME).await;
    let published: Result<i64, _> = pool.next().publish("env.events", FASTSTREAM_FRAME).await;
    let added: Result<String, _> = pool
        .xadd(
            "env.orders",
            false,
            None::<()>,
            "*",
            vec![("__data__", FASTSTREAM_FRAME)],
        )
        .await;
    if pushed.is_ok() && published.is_ok() && added.is_ok() {
        HandlerOutcome::ack()
    } else {
        HandlerOutcome::drop()
    }
}

fn faststream_app() -> impl App {
    RustStream::new(AppInfo::new("faststream-peer", "0.1.0")).with_broker(
        RedisBroker::standalone(URL).envelope(FastStream),
        |b| {
            b.include(faststream_producer);
            b.include(job);
            b.include(event);
            b.include(order);
        },
    )
}

/// What a faststream service wrote reaches the handler as its payload and its headers, on every
/// form.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_frame_faststream_wrote_reads_as_payload_and_headers() {
    let tb = TestApp::start(faststream_app()).await.expect("start");

    tb.broker::<RedisBroker<FastStream>>()
        .message(&Payload(b"go".to_vec()))
        .to("fs.trigger")
        .publish()
        .await
        .expect("publish");
    tb.settle().await.expect("settle");

    for name in FORMS {
        tb.broker::<RedisBroker<FastStream>>()
            .subscriber(name)
            .assert_called_once()
            .with_raw(ORDER)
            .settled(HandlerOutcome::ack());
    }

    tb.shutdown().await.expect("shutdown");
}

#[derive(OutSlot)]
#[publishes(Tagged)]
struct Peer;

/// Relays every order to `fs.out`, in faststream's frame, with the tenant header.
#[subscriber(RedisList::new("relay.in"))]
async fn relay(raw: &Raw<'_>, Out(peer): Out<impl Publisher, Peer>) -> HandlerOutcome {
    let sent = peer
        .message(&Tagged(raw.0.to_vec()))
        .to("fs.out")
        .with_headers(&acme())
        .publish()
        .await;
    if sent.is_ok() {
        HandlerOutcome::ack()
    } else {
        HandlerOutcome::retry()
    }
}

/// Reads `fs.out` with no envelope, as the bytes Redis holds.
#[subscriber(RedisList::new("fs.out"))]
async fn wire(raw: &Raw<'_>) -> HandlerOutcome {
    tracing::debug!(len = raw.0.len(), "wire");
    HandlerOutcome::ack()
}

fn relay_app() -> impl App {
    RustStream::new(AppInfo::new("relay", "0.1.0")).with_broker(RedisBroker::standalone(URL), |b| {
        b.include(relay)
            .out(Peer, RedisListPublish::new().envelope(FastStream))
            .build();
        b.include(wire.envelope(NoOp));
    })
}

/// What this service writes in faststream's frame is the frame faststream writes for the same
/// message, byte for byte, laid out by hand from the format: the identity header and version, the
/// offsets of the header block and the body, one header, the body.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_frame_this_service_writes_is_the_one_faststream_reads() {
    let mut expected = b"\x89BIN\r\n\x1a\n\x00\x01".to_vec();
    expected.extend_from_slice(&18u32.to_be_bytes());
    expected.extend_from_slice(&34u32.to_be_bytes());
    expected.extend_from_slice(b"\x00\x01\x00\x06tenant\x00\x04acme");
    expected.extend_from_slice(ORDER);
    let tb = TestApp::start(relay_app()).await.expect("start");

    tb.broker::<RedisBroker>()
        .message(&Payload(ORDER.to_vec()))
        .to("relay.in")
        .publish()
        .await
        .expect("publish");
    tb.settle().await.expect("settle");

    tb.broker::<RedisBroker>()
        .subscriber("fs.out")
        .assert_called_once()
        .with_raw(&expected)
        .settled(HandlerOutcome::ack());

    tb.shutdown().await.expect("shutdown");
}

/// Stands in for a client that knows no envelope: it writes bare JSON to the list and the channel
/// and a field map of its own to the stream.
#[subscriber(RedisList::new("foreign.trigger"))]
async fn foreign_producer(_trigger: &Raw<'_>, Ctx(pool): Ctx<keys::FredPool>) -> HandlerOutcome {
    let pushed: Result<i64, _> = pool.lpush("env.jobs", ORDER).await;
    let published: Result<i64, _> = pool.next().publish("env.events", ORDER).await;
    let added: Result<String, _> = pool
        .xadd("env.orders", false, None::<()>, "*", vec![("order", "7")])
        .await;
    if pushed.is_ok() && published.is_ok() && added.is_ok() {
        HandlerOutcome::ack()
    } else {
        HandlerOutcome::drop()
    }
}

fn foreign_app<Env: Envelope>(envelope: Env) -> impl App {
    RustStream::new(AppInfo::new("foreign", "0.1.0")).with_broker(
        RedisBroker::standalone(URL).envelope(envelope),
        |b| {
            b.include(foreign_producer);
            b.include(job);
            b.include(event);
            b.include(order);
        },
    )
}

/// A value that is not one of the envelope's own arrives as the payload with no headers, through
/// every envelope: the list element and the channel message whole, the stream entry as the JSON
/// object of its fields.
async fn foreign_values_arrive_whole<Env: Envelope>(envelope: Env) {
    let tb = TestApp::start(foreign_app(envelope)).await.expect("start");

    tb.broker::<RedisBroker<Env>>()
        .message(&Payload(b"go".to_vec()))
        .to("foreign.trigger")
        .publish()
        .await
        .expect("publish");
    tb.settle().await.expect("settle");

    for (name, payload) in [
        ("env.jobs", ORDER),
        ("env.events", ORDER),
        ("env.orders", br#"{"order":"7"}"#.as_slice()),
    ] {
        tb.broker::<RedisBroker<Env>>()
            .subscriber(name)
            .assert_called_once()
            .with_raw(payload)
            .settled(HandlerOutcome::drop());
    }

    tb.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_foreign_value_arrives_whole_through_the_default_envelope() {
    foreign_values_arrive_whole(envelope::RustStream).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_foreign_value_arrives_whole_through_faststream() {
    foreign_values_arrive_whole(FastStream).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_foreign_value_arrives_whole_through_a_codec_document() {
    foreign_values_arrive_whole(Framed::new(JsonCodec)).await;
}

#[derive(OutSlot)]
#[publishes(Payload)]
struct Bare;

/// Publishes through `NoOp` with a partition key, which travels as a header: the publish has to
/// fail and name the header and the destination.
#[subscriber(RedisList::new("bare.in"))]
async fn keyed_bare(
    raw: &Raw<'_>,
    Out(bare): Out<impl Publisher<Options = RedisPublishOptions>, Bare>,
) -> HandlerOutcome {
    let sent = bare
        .message(&Payload(raw.0.to_vec()))
        .to("bare.out")
        .partition_key("tenant-a")
        .publish()
        .await;
    match sent {
        Err(err) if refuses_the_key(&err) => HandlerOutcome::ack(),
        _ => HandlerOutcome::drop(),
    }
}

/// Whether `err`, or an error under it, names the partition key header and `bare.out`.
fn refuses_the_key(err: &(dyn std::error::Error + 'static)) -> bool {
    let text = err.to_string();
    (text.contains("redis-partition-key") && text.contains("bare.out"))
        || err.source().is_some_and(refuses_the_key)
}

fn bare_app() -> impl App {
    RustStream::new(AppInfo::new("bare", "0.1.0")).with_broker(RedisBroker::standalone(URL), |b| {
        b.include(keyed_bare)
            .out(Bare, RedisListPublish::new().envelope(NoOp))
            .build();
    })
}

/// `NoOp` has no room for a header, and a publish that carries one fails with the header and the
/// destination named, instead of reaching the list without it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_envelope_refuses_a_header_by_name() {
    let tb = TestApp::start(bare_app()).await.expect("start");

    tb.broker::<RedisBroker>()
        .message(&Payload(ORDER.to_vec()))
        .to("bare.in")
        .publish()
        .await
        .expect("publish");
    tb.settle().await.expect("settle");

    tb.broker::<RedisBroker>()
        .subscriber("bare.in")
        .assert_called_once()
        .settled(HandlerOutcome::ack());
    tb.broker::<RedisBroker>()
        .published::<Payload>("bare.out")
        .assert_not_called();

    tb.shutdown().await.expect("shutdown");
}

/// This crate's own layout, counting how often a delivery's headers are read.
#[derive(Debug, Clone, Default)]
struct Counting {
    reads: Arc<AtomicUsize>,
}

impl Envelope for Counting {
    const CONTENT_TYPE: &'static str = envelope::RustStream::CONTENT_TYPE;

    fn locate(&self, value: &[u8]) -> Location {
        envelope::RustStream.locate(value)
    }

    fn headers(&self, value: &[u8]) -> Option<HeaderMap> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        envelope::RustStream.headers(value)
    }

    fn seal(
        &self,
        payload: &[u8],
        headers: &HeaderMap,
        value: &mut Vec<u8>,
    ) -> Result<(), SealError> {
        envelope::RustStream.seal(payload, headers, value)
    }
}

ruststream::register_testable_broker!(RedisBroker<Counting>);

/// Reads the payload alone.
#[subscriber(RedisList::new("lazy.payload"))]
async fn payload_only(raw: &Raw<'_>) -> HandlerOutcome {
    tracing::debug!(len = raw.0.len(), "payload only");
    HandlerOutcome::ack()
}

/// Reads the headers too.
#[subscriber(RedisList::new("lazy.headers"))]
async fn with_headers(
    _raw: &Raw<'_>,
    Headers(headers): Headers<BTreeMap<String, String>>,
) -> HandlerOutcome {
    by_tenant(&headers)
}

/// A handler that never reads headers costs no parse of them; one that does reads them once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn headers_are_parsed_only_where_a_handler_reads_them() {
    let counting = Counting::default();
    let tb = TestApp::start(RustStream::new(AppInfo::new("lazy", "0.1.0")).with_broker(
        RedisBroker::standalone(URL).envelope(counting.clone()),
        |b| {
            b.include(payload_only);
            b.include(with_headers);
        },
    ))
    .await
    .expect("start");

    for name in ["lazy.payload", "lazy.payload", "lazy.headers"] {
        tb.broker::<RedisBroker<Counting>>()
            .message(&Tagged(ORDER.to_vec()))
            .to(name)
            .with_headers(&acme())
            .publish()
            .await
            .expect("publish");
    }
    tb.settle().await.expect("settle");

    tb.broker::<RedisBroker<Counting>>()
        .subscriber("lazy.payload")
        .assert_called(2)
        .settled(HandlerOutcome::ack());
    tb.broker::<RedisBroker<Counting>>()
        .subscriber("lazy.headers")
        .assert_called_once()
        .settled(HandlerOutcome::ack());
    assert_eq!(
        counting.reads.load(Ordering::SeqCst),
        1,
        "the headers were parsed for a delivery whose handler never read them"
    );

    tb.shutdown().await.expect("shutdown");
}

#[subscriber(RedisList::new("piped.jobs"))]
async fn piped(
    _raw: &Raw<'_>,
    Headers(headers): Headers<BTreeMap<String, String>>,
) -> HandlerOutcome {
    by_tenant(&headers)
}

/// Stands in for a faststream service writing faststream's frame to `piped.jobs`.
#[subscriber(RedisList::new("piped.trigger"))]
async fn piped_producer(_trigger: &Raw<'_>, Ctx(pool): Ctx<keys::FredPool>) -> HandlerOutcome {
    let pushed: Result<i64, _> = pool.lpush("piped.jobs", FASTSTREAM_FRAME).await;
    if pushed.is_ok() {
        HandlerOutcome::ack()
    } else {
        HandlerOutcome::drop()
    }
}

/// The mount site names the envelope after the window `.pipeline()` opened, and the subscription
/// reads through it while the broker keeps its own.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_mount_site_names_the_envelope_through_a_window() {
    let tb = TestApp::start(RustStream::new(AppInfo::new("piped", "0.1.0")).with_broker(
        RedisBroker::standalone(URL),
        |b| {
            b.include(piped_producer);
            b.include(piped.pipeline().envelope(FastStream));
        },
    ))
    .await
    .expect("start");

    tb.broker::<RedisBroker>()
        .message(&Payload(b"go".to_vec()))
        .to("piped.trigger")
        .publish()
        .await
        .expect("publish");
    tb.settle().await.expect("settle");

    tb.broker::<RedisBroker>()
        .subscriber("piped.jobs")
        .assert_called_once()
        .with_raw(ORDER)
        .settled(HandlerOutcome::ack());

    tb.shutdown().await.expect("shutdown");
}
