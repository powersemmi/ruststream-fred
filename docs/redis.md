# Redis

`ruststream-fred` is the Redis broker. It is built on Redis Streams: every subscription reads
through a consumer group, so deliveries are durable and acknowledged. It ships an in-memory test
broker under its `testing` feature. For framework concepts (writing subscribers, routing, codecs,
middleware), see the [RustStream documentation](https://powersemmi.github.io/ruststream/).

```toml
ruststream = { version = "0.4", features = ["macros"] }
ruststream-fred = "0.4"
serde = { version = "1", features = ["derive"] }
```

`RedisBroker::standalone` is synchronous and does no I/O, so a Redis service is assembled with the
same `#[ruststream::app]` macro as any other broker. The runtime connects the broker once at startup,
before opening subscriptions.

## A subscriber

A `#[subscriber("key")]` handler binds to a Redis stream key. Because Redis Streams always read
through a consumer group, the bare-string form needs a broker-wide default group (`.default_group`):

```rust
--8<-- "crates/ruststream-fred/examples/fred_streams.rs:handler"
```

Wire it onto the broker; the `with_broker` / `include` part is identical to every other broker.

```rust
--8<-- "crates/ruststream-fred/examples/fred_streams.rs:app"
```

Payload and headers travel as stream entry fields: the body under a reserved field and each header
under a `h:` prefix, so a round-trip through `XADD` / `XREADGROUP` preserves both.

## Read modes: fresh tail vs reclaim

The read mode is chosen by constructor, never a runtime flag, because the two return disjoint sets of
messages:

- `RedisStream::new(key)` reads fresh entries off the tail (`XREADGROUP >`). This is the normal
  worker.
- `RedisStream::reclaim(key, min_idle)` reclaims entries another consumer fetched but never acked
  (`XAUTOCLAIM`, idle at least `min_idle`). This is crash recovery, run alongside a `new` subscriber
  on the same group ("two handlers per group").

`min_idle` has no default and must exceed the longest legitimate handler runtime: set it too low and
a healthy consumer's in-flight message gets reclaimed and processed twice.

A descriptor can sit directly in the `#[subscriber(...)]` decorator. The fresh-tail worker:

```rust
--8<-- "crates/ruststream-fred/examples/fred_reclaim.rs:worker"
```

The recovery handler on the same group, reclaiming entries idle for over 30 seconds:

```rust
--8<-- "crates/ruststream-fred/examples/fred_reclaim.rs:reclaim"
```

## Acknowledgement

Settlement follows the republish-retry model:

- `ack` -> `XACK` (remove from the pending list).
- `nack(requeue = true)` -> re-append a copy to the same stream, then `XACK` the original. The copy
  is reprocessed by the normal `new` consumer. This is at-least-once: a crash between the two leaves
  a duplicate.
- `nack(requeue = false)` -> `XACK` to drop.

## Topologies

One crate, three named constructors. Each is synchronous and connects lazily:

```toml
# standalone
# RedisBroker::standalone("redis://localhost:6379")
# cluster (one reachable seed node is enough; the rest is discovered)
# RedisBroker::cluster(["127.0.0.1:7000", "127.0.0.1:7001"])
# sentinel (the monitored primary's name plus the sentinels)
# RedisBroker::sentinel("mymaster", ["127.0.0.1:26379"])
```

## Authentication and TLS

The standalone URL carries credentials (`redis://user:pass@host`), but the bare `cluster` /
`sentinel` seed lists cannot. Builders set them on every topology, mapping onto fred's config:

```rust
--8<-- "crates/ruststream-fred/examples/fred_auth.rs:credentials"
```

For a password-only `AUTH` (the legacy `requirepass` form, no ACL user) use `.password(...)`:

```rust
--8<-- "crates/ruststream-fred/examples/fred_auth.rs:password"
```

Credentials set programmatically override any in a standalone URL.

TLS lives behind additive, off-by-default features that map onto fred's TLS backends - `tls-rustls`
(rustls with aws-lc-rs), `tls-rustls-ring` (rustls with ring), and `tls-native-tls`. With one
enabled, pass a `TlsConfig` (or any `TlsConnector`) on any topology; a standalone broker can also
use a `rediss://` / `valkeys://` URL:

```rust
--8<-- "crates/ruststream-fred/examples/fred_tls.rs:tls"
```

Two further auth features are off by default:

- `sentinel-auth` adds `.sentinel_credentials(user, pass)` / `.sentinel_password(pass)` for
  credentials that authenticate to the sentinels, distinct from the data-node credentials.
- `credential-provider` accepts `.credential_provider(provider)`, a callback that supplies and can
  rotate the username/password on each `AUTH` / `HELLO` (IAM-style auth); it takes precedence over
  static credentials.

For full control (custom reconnection, performance, or TLS policy beyond these builders), build a
fred `Pool` yourself and wrap it with `RedisBroker::from_pool`.

## Transactions

On standalone and sentinel the stream publisher is transactional (`begin_transaction` buffers,
`commit` flushes the whole batch in publish order through a single `fred` pipeline, `abort`
discards). The idiomatic way to use it is a batch-publishing handler wired with a `.transactional()`
publisher: every reply is committed atomically.

```rust
--8<-- "crates/ruststream-fred/examples/fred_transaction.rs:batch"
```

```rust
--8<-- "crates/ruststream-fred/examples/fred_transaction.rs:mount"
```

Cluster does not offer this (buffered keys may hash to different nodes), so the transaction returns
an error there.

## Pub/Sub

Pub/Sub is fire-and-forget: no durability, no consumer groups, no ack (`ack` / `nack` report
`Unsupported`). A `RedisPubSub` descriptor selects the channel and mode. Classic broadcasts
cluster-wide and supports patterns:

```rust
--8<-- "crates/ruststream-fred/examples/fred_pubsub.rs:classic"
```

Sharded delivery (`SSUBSCRIBE`, Redis 7+) stays slot-local so it scales across a cluster, at the cost
of patterns. Enable it per subscription with `.mode(PubSubMode::Sharded)`:

```rust
--8<-- "crates/ruststream-fred/examples/fred_pubsub.rs:sharded"
```

Because RustStream is multi-broker, one service can run classic Pub/Sub on a standalone server and
sharded Pub/Sub on a cluster at the same time - each handler mounts on its own broker:

```rust
--8<-- "crates/ruststream-fred/examples/fred_pubsub.rs:app"
```

To publish, mount the handler with `include_publishing` and a `broker.pubsub_publisher()` (add
`.mode(PubSubMode::Sharded)` to match a sharded subscriber). The classic handler above uses the macro
`publish("audit")` form, so its return value goes out through that Pub/Sub publisher - not the default
stream publisher:

```rust
--8<-- "crates/ruststream-fred/examples/fred_pubsub.rs:app"
```

Headers travel in a frame around the payload: a lossless binary frame by default, or - when you set a
codec on both the publisher and the subscriber (`.codec(JsonCodec)`) - a readable codec-serialized
`{headers, payload}` envelope (so the wire value is legible JSON in tools like RedisInsight). A raw
value an external client published is delivered as the payload with empty headers.

## Lists (work queue)

A list is a competing-consumers queue: a producer `LPUSH`es, consumers pop from the right, and each
entry goes to exactly one consumer (no fan-out, no replay). Simple mode is at-most-once (`BRPOP`, no
ack):

```rust
--8<-- "crates/ruststream-fred/examples/fred_list.rs:simple"
```

Reliable mode moves each entry to a processing list and removes it on ack (at-least-once), so a
crashed handler does not silently lose its job:

```rust
--8<-- "crates/ruststream-fred/examples/fred_list.rs:reliable"
```

Publish with `broker.list_publisher()` (`LPUSH`). Headers travel in the same frame as Pub/Sub: a
lossless binary frame by default, or a readable codec-serialized envelope when a codec is set on both
ends (`.codec(JsonCodec)`). Recovering entries stranded on a processing list by a dead consumer (a
ZSET watchdog) is not in 0.4; for a durable, recoverable queue prefer Redis Streams.

## Testing

The `testing` feature ships `RedisTestBroker` / `RedisTestClient`, a handler-stub dispatcher that
routes by exact stream key with no server. It reproduces routing, ack/nack, and headers, and passes
the framework's conformance suite. It does not simulate consumer-group cursors, `XAUTOCLAIM`
redelivery, trimming, or dead-letter routing - exercise those against a real Redis server (see the
crate's `integration_fred` tests and `docker-compose.test.yml`).

```toml
[dev-dependencies]
ruststream-fred = { version = "0.4", features = ["testing"] }
```
