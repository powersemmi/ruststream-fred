# Redis broker

`ruststream-fred` is the Redis broker for the [RustStream](https://powersemmi.github.io/ruststream/)
framework. It is built on Redis Streams: every subscription reads through a consumer group, so
deliveries are durable and acknowledged. It also ships an in-memory test broker under its `testing`
feature.

```toml
ruststream = { version = "0.7", features = ["macros"] }
ruststream-fred = "0.7"
serde = { version = "1", features = ["derive"] }
```

`RedisBroker::standalone` is synchronous and does no I/O, so a Redis service is assembled with the
same `#[ruststream::app]` macro as any other broker. The runtime drives the lifecycle ladder at
startup: the consuming `connect` produces the `ConnectedRedisBroker` that subscriptions and
publishers hang off, and the consuming `shutdown` closes it. Publishers are declared as a policy
(`RedisPublish`, `RedisPubSubPublish`, `RedisListPublish`) that the runtime pairs with the connected
broker, so publishing before connect cannot be expressed.

## Scaffold a service

Generate a runnable starter with [`cargo generate`](https://github.com/cargo-generate/cargo-generate),
one template per transport:

```bash
cargo generate --git https://github.com/powersemmi/ruststream-fred templates/redis-stream
cargo generate --git https://github.com/powersemmi/ruststream-fred templates/redis-pubsub
cargo generate --git https://github.com/powersemmi/ruststream-fred templates/redis-list
```

## Topologies

One crate, three named constructors. Each is synchronous and I/O-free; the connection opens in
`connect`:

```toml
# standalone
# RedisBroker::standalone("redis://localhost:6379")
# cluster (one reachable seed node is enough; the rest is discovered)
# RedisBroker::cluster(["127.0.0.1:7000", "127.0.0.1:7001"])
# sentinel (the monitored primary's name plus the sentinels)
# RedisBroker::sentinel("mymaster", ["127.0.0.1:26379"])
```

## Transport guides

- [Redis Streams](streams.md) - consumer groups, fresh tail vs reclaim, pages, repositioning,
  delayed retry.
- [Redis Lists](lists.md) - competing-consumers work queue, reliable mode, orphan recovery.
- [Pub/Sub](pubsub.md) - classic and sharded broadcast.
- [Dead-letter and poison cap](dead-letter.md) - bound infinite redelivery.
- [Authentication and TLS](auth-tls.md) - credentials and TLS on every topology.
- [Transactions](transactions.md) - page publishing on standalone and sentinel.
- [Testing](testing.md) - in-process handler-stub broker.
