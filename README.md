<h1 align="center">ruststream-fred</h1>

<p align="center">
  <i>The Redis broker for the <a href="https://github.com/powersemmi/ruststream">RustStream</a> messaging framework: Redis Streams consumer groups, standalone / cluster / sentinel topologies, and an in-process test broker.</i>
</p>

<p align="center">
  <a href="https://github.com/powersemmi/ruststream-fred/actions/workflows/ci.yml"><img src="https://github.com/powersemmi/ruststream-fred/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://crates.io/crates/ruststream-fred"><img src="https://img.shields.io/crates/v/ruststream-fred.svg" alt="crates.io"></a>
  <a href="https://docs.rs/ruststream-fred"><img src="https://img.shields.io/docsrs/ruststream-fred" alt="docs.rs"></a>
  <img src="https://img.shields.io/badge/MSRV-1.88-blue.svg" alt="MSRV 1.88">
  <img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="License">
</p>

<p align="center">
  <b><a href="https://powersemmi.github.io/ruststream-fred/">Documentation</a></b>
</p>

---

`ruststream-fred` implements the RustStream broker contract over [`fred`](https://crates.io/crates/fred), using Redis Streams as the durable transport. Handlers, routers, codecs, and middleware come from the framework; this crate supplies the transport - and nothing broker-specific leaks back into the framework.

## Features

- **Redis Streams with consumer groups.** Subscribe through a group off the fresh tail
  (`RedisStream::new`), or reclaim a crashed consumer's pending entries (`RedisStream::reclaim`).
  Payload and headers round-trip as stream entry fields.
- **Standalone, cluster, and sentinel.** One crate, named constructors pick the topology:
  `RedisBroker::standalone`, `::cluster`, `::sentinel`.
- **Authentication and TLS on every topology.** `.credentials` / `.password` set the auth fields
  beyond what a standalone URL can express; optional features add TLS (`tls-rustls`,
  `tls-rustls-ring`, `tls-native-tls`), sentinel-specific auth (`sentinel-auth`), and a dynamic
  `credential-provider` for IAM-style rotation.
- **Lazy startup contract.** `RedisBroker::standalone(url)` is synchronous and does no I/O; the
  runtime connects once at startup, so the broker composes with `#[ruststream::app]`. An existing
  `fred` pool plugs in via `RedisBroker::from_pool`.
- **Acknowledgement via the republish-retry model.** `ack` is `XACK`; `nack(requeue = true)`
  re-appends a copy to the stream then acks the original (at-least-once); `nack(requeue = false)`
  acks to drop.
- **In-process test broker.** The `testing` feature ships `RedisTestBroker` / `RedisTestClient`, a
  handler-stub transport that passes the framework's conformance suite without a server.

## Install

```toml
[dependencies]
ruststream = { version = "0.4", features = ["macros", "json"] }
ruststream-fred = "0.4"
serde = { version = "1", features = ["derive"] }
```

## License

Apache-2.0.
