---
sidebar_position: 1
description: "Install ruststream-fred, start a local Redis, run a first subscriber on a Redis stream and test it without a server."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Getting started

This guide installs `ruststream-fred`, starts a local Redis and runs a first service against it.

## Installation

```toml title="Cargo.toml"
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json", "logging"] }
ruststream-fred = "0.7"
serde = { version = "1", features = ["derive"] }
tracing = "0.1"

[dev-dependencies]
ruststream-fred = { version = "0.7", features = ["testing"] }
tokio = { version = "1", features = ["macros", "rt"] }
```

`ruststream` is the framework: `macros` gives the attributes, `json` the default codec, and `logging` prints the service's log to the terminal.
`ruststream-fred` is the Redis broker, built on the [fred](https://docs.rs/fred) client.
Its `testing` feature adds the [in-process Redis](../components/testing/index.md) that tests run on.
`fred` is written in Rust, so the build needs no system library.

The broker crate has these features, all off by default:

| Feature | What it gives |
|---|---|
| `testing` | the in-process Redis behind `TestApp` |
| `asyncapi` | the [Redis extension](../components/asyncapi/index.md) in the generated AsyncAPI document |
| `tls-rustls`, `tls-rustls-ring`, `tls-native-tls` | [TLS](../components/broker/tls.md), on rustls with `aws-lc-rs`, on rustls with `ring`, or on the system's TLS library |
| `sentinel-auth` | separate [credentials for the sentinels](../components/broker/sentinel-credentials.md) |
| `credential-provider` | [rotating credentials](../components/broker/credential-provider.md) through a callback |

## A local Redis

One Redis server is enough to develop against:

```bash
docker run --name redis -p 6379:6379 redis:8.4
```

The server listens on `localhost:6379`.
Redis 8.4 serves every feature of the crate; the [claiming read mode](../components/streams/claiming.md) needs at least that version.
Valkey works the same way: `RedisBroker::standalone` takes a `valkey://` URL, and `valkeys://` for TLS.

## The first service

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/getting_started.rs#service"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/getting_started.rs#service"
```

</TabItem>
</Tabs>

`take_order` reads JSON orders from the Redis stream `orders`.
It acknowledges an order with an amount and drops one without.
[`RedisBroker::standalone`](../components/broker/index.md) takes the server's URL and only records it.
The connection opens when the service starts.
A Redis stream is read through a consumer group.
[`RedisStream::new("orders").group("orders-svc")`](../components/streams/index.md) names the stream and the group, and the service creates both at startup when they do not exist yet.
`#[ruststream::app]` turns `app` into `main`; in the manual form, [`run_main`](api:ruststream::runtime::cli::run_main) does the same.

Run the service:

```bash
cargo run -- run
```

Add an order to the stream from another terminal:

```bash
docker exec redis redis-cli XADD orders '*' _payload '{"id":1,"amount_cents":1250}'
```

The payload goes in the entry's `_payload` field.
The service logs `order taken` with the order's id.
A new group starts at the end of the stream, so the service reads the orders added after its first start.
<kbd>Ctrl</kbd>+<kbd>C</kbd> stops it.

`cargo generate` scaffolds the same kind of service with a router and a reply.
There is one template per Redis structure: `redis-stream`, `redis-list` and `redis-pubsub`.

```bash
cargo generate --git https://github.com/powersemmi/ruststream-fred templates/redis-stream --name my-service
```

## The first test

The example file also holds two tests.
Each starts the same `app` with `TestApp::start`, publishes an order and checks how `take_order` settled it.
`RedisBroker` connects to an in-process Redis there, so the tests need no server:

```bash
cargo test
```

[Testing a Redis service](testing.md) walks through the tests of a larger service and what the in-process Redis models.

## Coming from fred

A service on `fred` writes its own read loop, its acknowledgements and its group setup.
Here is where each part goes in RustStream:

| fred | RustStream |
|---|---|
| `Builder::from_config(..)` and a `Pool` | [`RedisBroker::standalone`](../components/broker/index.md), [`cluster`](../components/broker/cluster.md) or [`sentinel`](../components/broker/sentinel.md), with [`pool`](../components/broker/pool.md) for its size; [`from_pool`](../components/broker/from-pool.md) takes a pool you built |
| `XGROUP CREATE .. MKSTREAM` | done by the subscription at startup |
| an `XREADGROUP` loop | a `#[subscriber]` handler on [`RedisStream`](../components/streams/index.md) |
| decoding the entry's fields | the handler's parameter type, decoded by the codec |
| `XACK` after the work | the handler's [outcome](../components/acknowledgement/index.md) |
| an `XAUTOCLAIM` loop | [`RedisStream::claiming`](../components/streams/claiming.md) or [`reclaim`](../components/streams/reclaim.md) ([Consumer groups and crash recovery](consumer-groups.md)) |
| a `BLMOVE` worker with `LREM` | [`RedisList::reliable`](../components/lists/reliable.md) ([Work queues on lists](work-queues.md)) |
| a `SubscriberClient` and its message stream | [`RedisPubSub`](../components/pubsub/index.md) ([Fan-out with Pub/Sub](fan-out.md)) |
| `XADD`, `LPUSH`, `PUBLISH` | a reply or an `Out` parameter, sent through a [publish policy](../components/publishing/index.md) |
| `MULTI` / `EXEC` around a group of `XADD`s | a [transaction](../components/transactions/index.md) |
