# The generated document

A RustStream service describes itself as an AsyncAPI document, and this crate fills in what only
Redis knows about a channel: which structure carries its messages, and how that structure is
configured.

Turn the feature on in both crates:

```toml
ruststream = { version = ">=0.7.0-rc.6, <0.8.0", features = ["macros", "asyncapi"] }
ruststream-fred = { version = "0.7", features = ["asyncapi"] }
```

The AsyncAPI specification has a `redis` binding, and all four of its objects are empty: there is no
field in it for a consumer group, a read mode or a delivery mode. A binding key is a closed list, so
the crate writes an extension beside it instead, `x-ruststream-redis`, which the specification
allows at exactly the level a binding sits at.

A stream subscription reports the group and consumer it reads through, its read mode, and the idle
threshold the two claiming modes claim at:

```json
--8<-- "crates/ruststream-fred/tests/fixtures/asyncapi_stream_channel.json"
```

That is the body under `channels.orders.bindings.x-ruststream-redis`, and a test in the crate
asserts the document against this very file.

A list reports whether it acknowledges (`reliable`), the processing list an unfinished entry sits on
while it does, and the framing its headers travel in. A channel reports its delivery mode and
whether its address is a glob. A publisher reports the same vocabulary from the other side: the
delivery mode a `PUBLISH` goes out in, the expiry a list push re-arms, the framing it writes.

Every value comes from the descriptor or the policy alone, because the document is built before
anything connects. Two consequences are worth knowing. The Redis server version is not reported:
the client learns it from the handshake, which has not happened yet. And no credential can reach
the document, which matters because a document is published and shared: the server entry carries
the host and port a client dials, with the scheme, any `user:password@`, the database path and the
query stripped.
