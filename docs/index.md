# Redis broker

`ruststream-fred` runs a [RustStream](https://powersemmi.github.io/ruststream/) service on Redis.
Redis Streams is a log, like Kafka: a subscription reads it through a consumer group and
acknowledges each entry it handles. Lists and Pub/Sub are here as well, and the `testing` feature
ships an in-process test broker, so tests run without a Redis server.

```toml
ruststream = { version = "0.7", features = ["macros"] }
ruststream-fred = "0.7"
serde = { version = "1", features = ["derive"] }
```

`RedisBroker::standalone` is synchronous and does no I/O: the runtime opens the connection at
startup and closes it at shutdown.

You name a publish policy when you register a handler: `RedisPublish` for streams,
`RedisPubSubPublish` for channels, `RedisListPublish` for lists. The runtime constructs the
publisher from that policy on the connected broker, so publishing before connect is not
representable.

The policy covers the publish, with one setting left to the message itself. `XADD`, `LPUSH` and
`PUBLISH` carry the key or the channel and the value, so a handler body usually writes
`.message(&value).publish()` and nothing else; the exception is the partition key, a step on that
builder. A body that sets one imports this crate's prelude and names the options type in its bound;
see [partition keys](streams.md#partition-keys).

## Scaffold a service

Generate a runnable starter with [`cargo generate`](https://github.com/cargo-generate/cargo-generate),
one template per transport:

```bash
cargo generate --git https://github.com/powersemmi/ruststream-fred templates/redis-stream
cargo generate --git https://github.com/powersemmi/ruststream-fred templates/redis-pubsub
cargo generate --git https://github.com/powersemmi/ruststream-fred templates/redis-list
```

## Topologies

Three named constructors pick the topology:

```toml
# standalone
# RedisBroker::standalone("redis://localhost:6379")
# cluster (one reachable seed node is enough; the rest is discovered)
# RedisBroker::cluster(["127.0.0.1:7000", "127.0.0.1:7001"])
# sentinel (the monitored primary's name plus the sentinels)
# RedisBroker::sentinel("mymaster", ["127.0.0.1:26379"])
```

## Transport guides

- [Redis Streams](streams.md) - consumer groups, fresh tail vs reclaim, batches, repositioning,
  delayed retry.
- [Redis Lists](lists.md) - competing-consumers work queue, reliable mode, orphan recovery.
- [Pub/Sub](pubsub.md) - classic and sharded broadcast.
- [Dead-letter and poison cap](dead-letter.md) - bound infinite redelivery.
- [Authentication and TLS](auth-tls.md) - credentials and TLS on every topology.
- [Transactions](transactions.md) - batch publishing on standalone and sentinel.
- [Testing](testing.md) - run a service and its handlers in process, without a Redis server.
