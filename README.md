<h1 align="center">ruststream-fred</h1>

<p align="center">
  <i>The Redis and Valkey broker for the <a href="https://github.com/powersemmi/ruststream">RustStream</a> messaging framework: Streams with consumer groups, lists, Pub/Sub, standalone / cluster / sentinel topologies, and an in-process mode that runs a service's own app in its tests.</i>
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

`ruststream-fred` connects a RustStream service to Redis and Valkey over
[`fred`](https://crates.io/crates/fred). Handlers, routing, codecs and middleware come from the
framework; this crate is the transport.

## Features

- **Redis Streams with consumer groups,** including reclaiming a crashed consumer's pending
  entries.
- **Lists and Pub/Sub beside them:** a list as a work queue, at-most-once or reliable, and Pub/Sub
  fan-out, classic, sharded or by pattern.
- **Settlement that follows the transport:** `XACK` on a stream, a processing list for a reliable
  list, and `AckError::Unsupported` where Redis has nothing to settle.
- **Retry caps and dead letters** declared where the handler is mounted, on every transport.
- **Batches on every transport,** read with one `XREADGROUP` on a stream.
- **Pipelining on every form:** `.pipeline()` sends a fetch's settles and the handler's own
  commands in one round trip, and `.atomic()` makes them one `MULTI` / `EXEC`.
- **Standalone, cluster and sentinel,** with authentication and TLS on each.
- **Transactions** as one `MULTI` / `EXEC` block, and repositioning a consumer group with
  `start_at(..)`.
- **Tests on the production app without a server:** `TestApp` runs the service's own app with its
  `RedisBroker` connected to an in-process Redis.

## Install

```toml
[dependencies]
ruststream = { version = ">=0.7.0-rc.11, <0.8.0", features = ["macros", "json"] }
ruststream-fred = "0.7"
serde = { version = "1", features = ["derive"] }

[dev-dependencies]
ruststream-fred = { version = "0.7", features = ["testing"] }
```

Optional features: TLS (`tls-rustls`, `tls-rustls-ring`, `tls-native-tls`), `sentinel-auth` and
`credential-provider`.

## Write a service

```rust
use ruststream_fred::stream::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
struct Order {
    id: u64,
}

#[derive(Debug, Outgoing, Serialize)]
struct Confirmation {
    id: u64,
}

#[subscriber(RedisStream::new("orders").group("workers"), publish("confirmations"))]
async fn confirm(order: &Order) -> Confirmation {
    Confirmation { id: order.id }
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            b.include(confirm).out_reply(Publish);
        },
    )
}
```

`#[ruststream::app]` generates `main`, so the binary understands `run` and `asyncapi gen`.

Scaffold a fresh project from a template, one per transport:

```bash
cargo generate --git https://github.com/powersemmi/ruststream-fred templates/redis-stream
cargo generate --git https://github.com/powersemmi/ruststream-fred templates/redis-pubsub
cargo generate --git https://github.com/powersemmi/ruststream-fred templates/redis-list
```

## Test it

`TestApp` runs the service's own `app()` with its `RedisBroker` connected to an in-process Redis,
with no server.

```rust
use ruststream::testing::TestApp;

let tb = TestApp::start(app()).await?;

tb.broker::<RedisBroker>()
    .message(&Order { id: 7 })
    .to("orders")
    .publish()
    .await?;

tb.broker::<RedisBroker>()
    .subscriber("orders")
    .assert_called_once()
    .settled(HandlerOutcome::ack());
tb.broker::<RedisBroker>()
    .published::<Confirmation>("confirmations")
    .assert_called_once();
```

## Documentation

- This crate: <https://docs.rs/ruststream-fred>
- The framework: <https://powersemmi.github.io/ruststream/latest>

## Minimum supported Rust version

The MSRV is **1.88**, edition 2024.

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md).

## License

Licensed under the [Apache-2.0](./LICENSE) license.
