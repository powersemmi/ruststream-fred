# ruststream-fred

Redis / Valkey broker implementation for the [RustStream](../..) messaging framework, backed by
[`fred`](https://crates.io/crates/fred). Built on Redis Streams: durable consumer groups with
acknowledgement, redelivery, and crash recovery, across standalone, cluster, and sentinel
topologies.

## Testing

```toml
[dev-dependencies]
ruststream-fred = { version = "*", features = ["testing"] }
```

`features = ["testing"]` exposes `RedisTestBroker`, an in-process test broker implementing
`ruststream::testing::TestableBroker` (exact stream-key routing, no server). Never enable this
feature in production builds.
