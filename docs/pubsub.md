# Pub/Sub

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
