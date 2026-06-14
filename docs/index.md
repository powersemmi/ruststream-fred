# ruststream-fred

**`ruststream-fred`** is the Redis broker for the
[RustStream](https://powersemmi.github.io/ruststream/) messaging framework. It is built on Redis
Streams: durable consumer groups with acknowledgement, redelivery, and crash recovery, across
standalone, cluster, and sentinel topologies. It ships an in-process test broker under its `testing`
feature.

Handlers, routers, codecs, and middleware come from the framework; this crate supplies the
transport, and nothing broker-specific leaks back into the framework.

```toml
ruststream = { version = "0.4", features = ["macros", "json"] }
ruststream-fred = "0.4"
serde = { version = "1", features = ["derive"] }
```

```rust
--8<-- "crates/ruststream-fred/examples/fred_streams.rs:app"
```

## Where to go next

<div class="grid cards" markdown>

- :material-database: **[Redis guide](redis.md)** - streams, consumer groups, reclaim, topologies, and testing.
- :material-book-open-variant: **[RustStream docs](https://powersemmi.github.io/ruststream/)** - the framework itself: subscribers, routing, codecs, middleware, the CLI.
- :material-language-rust: **[API reference](https://docs.rs/ruststream-fred)** - the crate's rustdoc on docs.rs.

</div>

## How this site relates to the RustStream docs

This site documents the Redis broker only. Framework concepts that apply to every broker (writing
subscribers, publishing, routing, codecs, middleware, observability, the CLI) live in the
[RustStream documentation](https://powersemmi.github.io/ruststream/). The pages here cover what is
specific to Redis and link back to the framework docs where the two meet.
