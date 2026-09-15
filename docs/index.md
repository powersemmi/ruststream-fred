# Redis broker

`ruststream-fred` runs a [RustStream](https://powersemmi.github.io/ruststream/) service on Redis.
Redis Streams is a log, like Kafka: a subscription reads it through a consumer group and
acknowledges each entry it handles. Lists and Pub/Sub are here as well, and the `testing` feature
ships an in-process test broker, so tests run without a Redis server.

```toml
ruststream = { version = ">=0.7.0-rc.7, <0.8.0", features = ["macros"] }
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
see
[partition keys](https://docs.rs/ruststream-fred/latest/ruststream_fred/index.html#partition-keys).

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

```rust
--8<-- "crates/ruststream-fred/examples/fred_topologies.rs:topologies"
```

## Where the rest is

The reference on docs.rs opens with the crate's own textbook, one section per topic:

- [Subscribing](https://docs.rs/ruststream-fred/latest/ruststream_fred/index.html#subscribing):
  the descriptors and what each answers about its redelivery, per transport
  ([streams](https://docs.rs/ruststream-fred/latest/ruststream_fred/index.html#streams),
  [lists](https://docs.rs/ruststream-fred/latest/ruststream_fred/index.html#lists),
  [Pub/Sub](https://docs.rs/ruststream-fred/latest/ruststream_fred/index.html#pubsub)), with
  [batches](https://docs.rs/ruststream-fred/latest/ruststream_fred/index.html#batches),
  [native delivery fields](https://docs.rs/ruststream-fred/latest/ruststream_fred/index.html#native-delivery-fields),
  [delayed retry](https://docs.rs/ruststream-fred/latest/ruststream_fred/index.html#delayed-retry),
  [capping the retries](https://docs.rs/ruststream-fred/latest/ruststream_fred/index.html#capping-the-retries)
  and
  [repositioning a group](https://docs.rs/ruststream-fred/latest/ruststream_fred/index.html#repositioning-a-group).
- [Publishing](https://docs.rs/ruststream-fred/latest/ruststream_fred/index.html#publishing): the
  policies, the
  [partition key](https://docs.rs/ruststream-fred/latest/ruststream_fred/index.html#partition-keys)
  step and
  [transactions](https://docs.rs/ruststream-fred/latest/ruststream_fred/index.html#transactions).
- [The generated document](https://docs.rs/ruststream-fred/latest/ruststream_fred/index.html#the-generated-document):
  what the AsyncAPI document says about a Redis channel, and what it deliberately leaves out.
- [Testing](https://docs.rs/ruststream-fred/latest/ruststream_fred/testing/index.html): the
  in-process transport, what it reproduces and what belongs in a test against a real server.
- [Operations](https://docs.rs/ruststream-fred/latest/ruststream_fred/index.html#operations):
  topologies, credentials, TLS and the known limits.

Handlers, routers, codecs and middleware come from the framework, whose own entry pages start at
[the RustStream site](https://powersemmi.github.io/ruststream/).
