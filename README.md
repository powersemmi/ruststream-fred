<h1 align="center">ruststream-fred</h1>

<p align="center">
  <i>The Redis and Valkey broker for the <a href="https://github.com/powersemmi/ruststream">RustStream</a> messaging framework: Streams with consumer groups, lists, Pub/Sub, standalone / cluster / sentinel topologies, and an in-process test broker.</i>
</p>

<p align="center">
  <a href="https://github.com/powersemmi/ruststream-fred/actions/workflows/ci.yml"><img src="https://github.com/powersemmi/ruststream-fred/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://crates.io/crates/ruststream-fred"><img src="https://img.shields.io/crates/v/ruststream-fred.svg" alt="crates.io"></a>
  <a href="https://crates.io/crates/ruststream-fred"><img src="https://img.shields.io/crates/dr/ruststream-fred" alt="Recent downloads"></a>
  <a href="https://docs.rs/ruststream-fred"><img src="https://img.shields.io/docsrs/ruststream-fred" alt="docs.rs"></a>
  <img src="https://img.shields.io/badge/MSRV-1.88-blue.svg" alt="MSRV 1.88">
  <img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="License">
</p>

<p align="center">
  <b><a href="https://powersemmi.github.io/ruststream-fred/">Documentation</a></b>
</p>

---

`ruststream-fred` implements the RustStream broker contract over [`fred`](https://crates.io/crates/fred). Redis Streams are the durable transport; lists and Pub/Sub sit beside them for work queues and fan-out. Handlers, routers, codecs, and middleware come from the framework; this crate supplies the transport - and nothing broker-specific leaks back into the framework.

## Features

- **Redis Streams with consumer groups.** Subscribe through a group off the fresh tail
  (`RedisStream::new`), or reclaim a crashed consumer's pending entries (`RedisStream::reclaim`).
  Payload and headers round-trip as stream entry fields.
- **Lists and Pub/Sub beside them.** `RedisList` is a competing-consumers work queue: `BRPOP`
  at-most-once, or `reliable()` for at-least-once through a per-consumer processing list.
  `RedisPubSub` is fire-and-forget fan-out, `Classic` broadcast or `Sharded` (`SSUBSCRIBE`,
  Redis 7+) so it scales across a cluster.
- **Settlement follows the transport.** On a stream `ack` is `XACK`; `nack(requeue = true)`
  re-appends a copy to the stream then acks the original (at-least-once); `nack(requeue = false)`
  acks to drop. A reliable list `LREM`s the entry off its processing list on ack and returns it to
  the main list on requeue. Simple lists and Pub/Sub have nothing to settle, so they report
  `AckError::Unsupported` rather than silently succeeding.
- **Batches on every transport.** A batch handler names its size where it is mounted
  (`batch(nonzero!(n))`); on a stream that number is the `COUNT` of the `XREADGROUP` that fetches
  the batch, while lists and Pub/Sub pop one entry at a time and assemble the batch on the client.
  Redis's own read option, `block(..)`, chains after the size on the stream and list forms.
- **One prelude per transport.** `stream::prelude`, `list::prelude`, and `pubsub::prelude` each
  carry the core prelude, that form's descriptor and options, and that form's publish policy under
  the uniform name `Publish` (streams add `TransactionalPublish` for the same policy), so a mount
  reads the same whichever form it is on. `ruststream_fred::prelude` spans all three: it keeps the
  prefixed names and re-exports the three form modules for a file that mixes them.
- **Standalone, cluster, and sentinel.** One crate, named constructors pick the topology:
  `RedisBroker::standalone`, `::cluster`, `::sentinel`.
- **Authentication and TLS on every topology.** `.credentials` / `.password` set the auth fields
  beyond what a standalone URL can express; optional features add TLS (`tls-rustls`,
  `tls-rustls-ring`, `tls-native-tls`), sentinel-specific auth (`sentinel-auth`), and a dynamic
  `credential-provider` for IAM-style rotation.
- **Typed lifecycle.** `RedisBroker::standalone(url)` is synchronous and does no I/O; the consuming
  `connect` yields a `ConnectedRedisBroker` that every subscription and publisher hangs off, and its
  consuming `shutdown` yields the terminal witness. The runtime drives the ladder at startup, so the
  broker composes with `#[ruststream::app]`. An existing `fred` pool plugs in via
  `RedisBroker::from_pool`.
- **Publishers as policy plus connection.** `RedisPublish`, `RedisPubSubPublish`, and
  `RedisListPublish` are pure declarations, constructible anywhere; a mount site binds one with
  `.out(marker, policy)`, and the runtime pairs it with the connected broker, so publishing before
  connect is not representable.
- **The partition key is a per-message setting.** `.partition_key(key)` is a step on the publish, so
  it keys one message and leaves the mount site's codec and its slot attribution alone. It feeds the
  runtime's keyed worker lanes (`workers(n, by_key)`) and reaches the consumer as the
  `redis-partition-key` header, on all three transports.
- **Both transaction kinds.** On standalone and sentinel the stream publisher carries the borrowed
  kind (one transaction on the handle) and the owned kind (`publisher.transaction()` returns a
  buffer-owning value, so any number can be open concurrently). Both commit their buffer as one
  `MULTI` / `EXEC` block, so subscribers see the whole batch or none of it.
- **Repositioning a group.** The streams subscriber implements the `Seekable` capability: a
  `start_at(..)` clause opens a subscription at a chosen point, and the delivery's own typed context
  carries the group's seeker under a `SeekHandle` key, so a handler moves the cursor while the
  service runs. A Redis cursor belongs to the consumer group, so a seek repositions every consumer
  of that group, a scope the `RedisGroupPosition` / `RedisGroupSeeker` names carry.
- **In-process test broker.** The `testing` feature ships `RedisTestBroker`, an in-process transport
  whose connected form implements `ruststream::testing::TestableBroker`, so it drives the `TestApp`
  harness and passes the framework's conformance suite without a server. Every descriptor and every
  publish policy mounts on it, so a test wires what the service ships rather than a bare key string
  and a test-only policy.

## Install

```toml
[dependencies]
ruststream = { version = ">=0.7.0-rc.4, <0.8.0", features = ["macros", "json"] }
ruststream-fred = "0.7"
serde = { version = "1", features = ["derive"] }

[dev-dependencies]
ruststream-fred = { version = "0.7", features = ["testing"] }
```

## Scaffold a service

Generate a runnable starter with [`cargo generate`](https://github.com/cargo-generate/cargo-generate),
one template per Redis transport:

```bash
# consumer-group streams (durable, acknowledged)
cargo generate --git https://github.com/powersemmi/ruststream-fred templates/redis-stream
# pub/sub fan-out (fire-and-forget)
cargo generate --git https://github.com/powersemmi/ruststream-fred templates/redis-pubsub
# list work queue (competing consumers)
cargo generate --git https://github.com/powersemmi/ruststream-fred templates/redis-list
```

## Write a service

```rust
use ruststream_fred::stream::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

#[derive(Debug, Outgoing, Serialize)]
struct Confirmation {
    id: u64,
}

// The subscription reads through the `workers` consumer group, so the entry is `XACK`ed once the
// handler returns, and the value it returns is published to the `confirmations` stream.
#[subscriber(RedisStream::new("orders").group("workers"), publish("confirmations"))]
async fn confirm(order: &Order) -> Confirmation {
    Confirmation { id: order.id }
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            // `.out(Reply, ..)` binds the policy the returned value leaves through. A policy holds
            // no connection, so the runtime pairs it with the broker once that connects.
            b.include(confirm).out(Reply, Publish);
        },
    )
}
```

Two vocabularies meet at that mount, and they do not share names. This file globs `stream::prelude`,
where `Publish` is the form's own policy - `list::prelude` and `pubsub::prelude` spell theirs with
the same word, so moving a handler between transports rewrites the descriptor and not the mount. A
handler that takes an injected publisher imports `ruststream::prelude::*` instead and bounds the
slot with a capability (`Out<impl Publisher>`, `Out<impl TransactionalPublisher>`), so its body
never names a Redis type.

Runnable examples live in `crates/ruststream-fred/examples/`, one per subject: `fred_streams`,
`fred_list`, `fred_pubsub`, `fred_transaction`, `fred_seek`, `fred_dead_letter`, `fred_auth`.

## Test it

`RedisTestBroker` runs the handler in process - no server, no docker, no network. The `TestApp`
harness performs the app's real startup and drives each publish to a standstill before returning,
so the assertions need no waiting. `Order` derives `Serialize` here as well, so the harness can
inject one:

```rust
use ruststream::testing::TestApp;
use ruststream_fred::testing::{RedisTestBroker, RedisTestPublish};

let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
    RedisTestBroker::new(),
    |b| {
        // The same handler and the same mount verb: only the policy names the transport.
        b.include(confirm).out(Reply, RedisTestPublish::default());
    },
);

let tb = TestApp::start(app).await?;

tb.broker::<RedisTestBroker>()
    .publish("orders", &Order { id: 7 })
    .await?;

tb.broker::<RedisTestBroker>()
    .subscriber("orders")
    .assert_called_once()
    .settled(HandlerOutcome::ack());
tb.broker::<RedisTestBroker>()
    .published::<Confirmation>("confirmations")
    .assert_called_once();

tb.shutdown().await?;
```

Full compiling example: `crates/ruststream-fred/examples/fred_testing.rs`. Consumer-group cursors,
`XAUTOCLAIM` redelivery, idle reclaim, and dead-letter routing are deliberately not simulated;
exercise those against a real server with `just test-brokers`.

## Contributing

```bash
just check          # fmt, clippy, and feature checks
just test           # the suite; the live Redis tests skip without REDIS_TEST_URL
just test-brokers   # the same suite against a Redis started with docker compose
```

## License

Apache-2.0.
