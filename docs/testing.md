# Testing

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

The `testing` feature ships `RedisTestBroker`, an in-process transport that routes by exact stream
key with no server. Its connected form implements `ruststream::testing::TestableBroker`, so the same
transport drives the `TestApp` harness and the conformance suite. It reproduces routing, ack/nack,
and headers. It does not simulate consumer-group cursors, `XAUTOCLAIM` redelivery, trimming, or
dead-letter routing - exercise those against a real Redis server (see the crate's `integration_fred`
tests and `docker-compose.test.yml`).

```toml
[dev-dependencies]
ruststream-fred = { version = "0.6", features = ["testing"] }
```

## Unit-testing a handler

Because a `#[subscriber]` handler is wired through a `RustStream` app, the most realistic in-process
test builds the same app around a `RedisTestBroker` and hands it to `TestApp`. Publishing through the
harness handle drives the reaction to quiescence, so the assertions need no waiting.

### Business-logic test

A real handler validates input, persists valid messages through a repository connector, and drops
invalid ones. The handler has no knowledge of the test harness.

```rust reference="examples/fred_testing.rs#repository"
```

```rust reference="examples/fred_testing.rs#business-handler"
```

The test publishes a valid payment and an invalid payment, then asserts that only the valid one
was saved:

```rust reference="examples/fred_testing.rs#business-test"
```

In your own crate you usually copy the test body into a `#[tokio::test]` inside a `#[cfg(test)]`
module:

```rust reference="examples/fred_testing.rs#unit-test"
```

### Transport-specific examples

<Tabs>
<TabItem value="redis-stream" label="Redis Stream">

```rust reference="examples/fred_testing.rs#stream-handler"
```

```rust reference="examples/fred_testing.rs#stream-test"
```

</TabItem>
<TabItem value="redis-list" label="Redis List">

```rust reference="examples/fred_testing.rs#list-handler"
```

```rust reference="examples/fred_testing.rs#list-test"
```

</TabItem>
<TabItem value="pub-sub" label="Pub/Sub">

```rust reference="examples/fred_testing.rs#pubsub-handler"
```

```rust reference="examples/fred_testing.rs#pubsub-test"
```

</TabItem>
</Tabs>

## Conformance suite

Run the framework's full conformance suite against the stub broker:

```rust reference="examples/fred_testing.rs#conformance"
```
