# Testing

The `testing` feature ships `RedisTestBroker`, an in-process transport that routes by exact stream
key, with no server. It reproduces routing, acknowledgement and headers. It does not reproduce
consumer-group cursors, `XAUTOCLAIM` redelivery, trimming or dead-letter routing, so those belong in
a test against a real server.

```toml
[dev-dependencies]
ruststream-fred = { version = "0.7", features = ["testing"] }
```

## Unit-testing a handler

A `#[subscriber]` handler runs inside a `RustStream` app, so a test builds the same app around a
`RedisTestBroker` and hands it to `TestApp`. Publishing through the harness handle drives the
reaction to a standstill, so the assertions after it need no waiting.

### Business-logic test

A handler validates its input, saves what is valid through a repository connector, and drops the
rest. Nothing in it knows about the test harness.

```rust
--8<-- "crates/ruststream-fred/examples/fred_testing.rs:repository"
```

```rust
--8<-- "crates/ruststream-fred/examples/fred_testing.rs:business-handler"
```

The test publishes a valid payment and an invalid payment, then asserts that only the valid one
was saved:

```rust
--8<-- "crates/ruststream-fred/examples/fred_testing.rs:business-test"
```

In your own crate the same body goes into a `#[tokio::test]` in a `#[cfg(test)]` module:

```rust
--8<-- "crates/ruststream-fred/examples/fred_testing.rs:unit-test"
```

### Transport-specific examples

=== "Redis Stream"

    ```rust
    --8<-- "crates/ruststream-fred/examples/fred_testing.rs:stream-handler"
    ```

    ```rust
    --8<-- "crates/ruststream-fred/examples/fred_testing.rs:stream-test"
    ```

=== "Redis List"

    ```rust
    --8<-- "crates/ruststream-fred/examples/fred_testing.rs:list-handler"
    ```

    ```rust
    --8<-- "crates/ruststream-fred/examples/fred_testing.rs:list-test"
    ```

=== "Pub/Sub"

    ```rust
    --8<-- "crates/ruststream-fred/examples/fred_testing.rs:pubsub-handler"
    ```

    ```rust
    --8<-- "crates/ruststream-fred/examples/fred_testing.rs:pubsub-test"
    ```

## What the stand-in mounts, ignores and refuses

Every descriptor and every publish policy this crate ships mounts on `RedisTestBroker`, so a test
wires the declaration the service ships. The three handlers above carry `RedisStream`, `RedisList`
and `RedisPubSub` as a routes file writes them, with no bare key string and no remapping at the
mount site. The reply half is the same value production names:

```rust
--8<-- "crates/ruststream-fred/examples/fred_testing.rs:reply-handler"
```

```rust
--8<-- "crates/ruststream-fred/examples/fred_testing.rs:reply-test"
```

`Publish` from each form's prelude pairs against the connected test broker as it pairs against a real
one, and it is also the stand-in's default reply publisher, so a handler that replies without naming
a policy works here. There is no test-only publish policy.

The stand-in routes by key or channel and nothing else. Consumer and group names, `start_id`,
`block`, `dead_letter`, `max_deliveries`, `delayed_retry`, a list's `reliable` processing list and
orphan-recovery watchdog, a Pub/Sub `mode` and the envelope `codec` change nothing in process. The
publish side is the same: a list `ttl` has no key to expire, and the envelope codec never runs, so a
reply reads back as the bare payload here and as a frame on a real server. Assert on any of those
against a real server.

Capabilities match per form. The stream policy pairs into a publisher carrying both transaction
kinds, as `RedisPublisher` does; the list and Pub/Sub policies pair into `RedisTestPlainPublisher`,
which offers `Publisher` alone, as `RedisListPublisher` and `RedisPubSubPublisher` do. A slot bounded
on a transaction capability fails to compile in process exactly where it fails against a real server,
instead of passing under the harness and breaking on the production build.

Two misconfigurations are caught here the way Redis catches them: a stream with no consumer group,
and a list recovery ZSET with no `min_idle`. Two descriptors are refused outright, because honouring
them in process would deliver what the real subscription never delivers:

- `RedisStream::reclaim(..)` reads another consumer's stale pending entries. The stand-in keeps no
  pending list, so the mount would hand the handler fresh entries instead.
- `RedisPubSub::pattern()` subscribes to a glob. The stand-in matches channel names exactly, so the
  mount would go silent on every channel the glob is meant to catch.

Settlement follows the transport. A stream and a reliable list acknowledge, and a requeue redelivers;
Pub/Sub and a simple list report `AckError::Unsupported` and refuse a requeue, here exactly as on a
real server. A handle that outlived `shutdown` refuses with `RedisError::ShutDown` rather than
writing into a router nobody is reading.

## Against a real server

The crate's own live suite lives in `tests/integration_fred.rs` and covers what an in-process
transport cannot: real consumer groups, `XACK`, the republish-on-nack path, `XAUTOCLAIM` reclaim,
and the cluster and sentinel topologies. Each topology is gated behind its own environment variable,
so a `cargo test` with none set needs no server. `docker-compose.test.yml` brings all three up, and
`just test-brokers` starts them and runs the suite.
