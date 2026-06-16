# Testing

The `testing` feature ships `RedisTestBroker` / `RedisTestClient`, a handler-stub dispatcher that
routes by exact stream key with no server. It reproduces routing, ack/nack, and headers, and passes
the framework's conformance suite. It does not simulate consumer-group cursors, `XAUTOCLAIM`
redelivery, trimming, or dead-letter routing - exercise those against a real Redis server (see the
crate's `integration_fred` tests and `docker-compose.test.yml`).

```toml
[dev-dependencies]
ruststream-fred = { version = "0.4", features = ["testing"] }
```

## Unit-testing a handler

Because a `#[subscriber]` handler is wired through a `RustStream` app, the most realistic in-process
test builds the same app around a `RedisTestBroker` and drives publishes through the test client.
The service runs until the test signals shutdown.

### Business-logic test

A real handler validates input, persists valid messages through a repository connector, and drops
invalid ones. The handler has no knowledge of the test harness.

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

In your own crate you usually copy the test body into a `#[tokio::test]` inside a `#[cfg(test)]`
module:

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

## Conformance suite

Run the framework's full conformance suite against the stub broker:

```rust
--8<-- "crates/ruststream-fred/examples/fred_testing.rs:conformance"
```
