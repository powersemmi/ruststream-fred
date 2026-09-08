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

Every descriptor and every publish policy this crate ships mounts on `RedisTestBroker`, so the test
wires the declaration the service ships. The three handlers above carry `RedisStream`, `RedisList`
and `RedisPubSub` exactly as a routes file writes them, with no bare key string and no remapping at
the mount site, and the reply half is the same value production names:

```rust
b.include(confirm).out(Reply, Publish);
```

There is no test-only publish policy. `Publish` from each form's prelude pairs against the connected
test broker as it pairs against the real one, and it is also the stand-in's default reply publisher,
so a handler that replies without naming a policy works here too.

The stand-in routes by key or channel and nothing else, so the rest of a descriptor has no effect
here: consumer and group names, `start_id`, `block`, `dead_letter`, `max_deliveries`,
`delayed_retry`, a list's `reliable` processing list and orphan-recovery watchdog, a Pub/Sub `mode`,
and the envelope `codec` on lists and Pub/Sub. The same holds on the publish side: a list `ttl` has
no key to expire in process, and the list and Pub/Sub envelope codecs never run, so a reply reads
back as the bare payload here and as a frame on a real server. A test that needs to assert on any of
those needs a real server.

One capability note: the stand-in has a single publisher, and it carries both transaction kinds. A
list or Pub/Sub reply slot therefore offers transactions in process that `RedisListPublisher` and
`RedisPubSubPublisher` do not, so a slot bounded on a transaction capability compiles here and not
against `RedisBroker`. That is a compile error at the same mount site either way, never a silent
difference at run time.

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
