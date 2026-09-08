# Testing

The `testing` feature ships `RedisTestBroker`, an in-process transport that routes by exact stream
key with no server. Its connected form implements `ruststream::testing::TestableBroker`, so the same
transport drives the `TestApp` harness and the conformance suite. It reproduces routing, ack/nack,
and headers. It does not simulate consumer-group cursors, `XAUTOCLAIM` redelivery, trimming, or
dead-letter routing - exercise those against a real Redis server (see the crate's `integration_fred`
tests and `docker-compose.test.yml`).

```toml
[dev-dependencies]
ruststream-fred = { version = "0.7", features = ["testing"] }
```

## Unit-testing a handler

Because a `#[subscriber]` handler is wired through a `RustStream` app, the most realistic in-process
test builds the same app around a `RedisTestBroker` and hands it to `TestApp`. Publishing through the
harness handle drives the reaction to quiescence, so the assertions need no waiting.

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

## What the stand-in mounts, ignores and refuses

Every descriptor this crate ships mounts on `RedisTestBroker`, so the test wires the declaration the
service ships: the three handlers above carry `RedisStream`, `RedisList` and `RedisPubSub` exactly as
a routes file writes them, with no bare key string and no remapping at the mount site.

The stand-in routes by key or channel and nothing else, so the rest of a descriptor has no effect
here: consumer and group names, `start_id`, `block`, `dead_letter`, `max_deliveries`,
`delayed_retry`, a list's `reliable` processing list and orphan-recovery watchdog, a Pub/Sub `mode`,
and the envelope `codec` on lists and Pub/Sub. A test that needs to assert on any of those needs a
real server.

Two things are validated the way the real broker validates them, so a subscription that could not
start against Redis does not start under the harness either: a stream with no consumer group, and a
list recovery ZSET with no `min_idle`.

Two are refused outright, because honouring them in process would deliver what the real subscription
never delivers:

- `RedisStream::reclaim(..)`, which reads another consumer's stale pending entries. The stand-in
  keeps no pending list, so the mount would hand the handler fresh entries instead.
- `RedisPubSub::pattern()`, which subscribes to a glob. The stand-in matches channel names exactly,
  so the mount would go silent on every channel the glob is meant to catch.

One difference is left for the test author to keep out of assertions: Pub/Sub and a simple
(non-reliable) list report `AckError::Unsupported` on a real server, while every in-process delivery
settles. Assert on what the handler did, not on a settlement the transport cannot perform.

## Conformance suite

Run the framework's full conformance suite against the stub broker:

```rust
--8<-- "crates/ruststream-fred/examples/fred_testing.rs:conformance"
```
