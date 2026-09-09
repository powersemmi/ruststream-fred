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

## Against a real server

The crate's own live suite lives in `tests/integration_fred.rs` and covers what an in-process
transport cannot: real consumer groups, `XACK`, the republish-on-nack path, `XAUTOCLAIM` reclaim,
and the cluster and sentinel topologies. Each topology is gated behind its own environment variable,
so a `cargo test` with none set needs no server. `docker-compose.test.yml` brings all three up, and
`just test-brokers` starts them and runs the suite.
