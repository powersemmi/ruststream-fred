---
sidebar_position: 12
description: "Test a Redis service in process with TestApp: replies, the handler's own Redis commands, delays on a paused clock, and the same test against a server."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Testing a Redis service

A test runs the service's own app with [`TestApp`](api:ruststream::testing::TestApp), against an [in-process Redis](../components/testing/index.md) or against a server.

The service under test charges card payments.
A payment arrives on the stream `card-payments`, and the handler replies with a receipt on `card-receipts`.
A payment without an amount is dropped.
Each account may spend 1,000.00 a day: the handler adds the amount to the key `spent:<account>` through the broker's [connection pool](../components/context/fred-pool.md), and holds a payment over the limit for a minute with [`retry_after`](../components/acknowledgement/retry-after.md).
After three deliveries a payment goes to `card-payments.review`, the [dead-letter](../components/acknowledgement/dead-letter.md) stream.
A refund arrives on the reliable list `card-refunds` and gives the amount back.
The app is built by `service(url)`, so a test chooses the server and `main` uses the production one.
The handler calls the pool through `fred`'s command traits, so the service lists `fred` among its dependencies.

The tests need the `testing` feature of `ruststream-fred` and a Tokio runtime:

```toml title="Cargo.toml"
[dev-dependencies]
ruststream-fred = { version = "0.7", features = ["testing"] }
tokio = { version = "1", features = ["macros", "rt", "rt-multi-thread"] }
```

## A payment and its receipt

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/testing.rs#receipt"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#receipt"
```

</TabItem>
</Tabs>

[`TestApp::start`](api:ruststream::testing::TestApp::start) connects [`RedisBroker`](../components/broker/index.md) to a Redis modelled inside the test process.
The URL is never dialled.
The test addresses the broker by its production type, `tb.broker::<RedisBroker>()`.
A test's message goes where the broker's default publisher writes its name: an `XADD` to a stream, or the `LPUSH` or `PUBLISH` of the subscription that reads the name.
`publish` returns once the handler and its reply have settled.
So the next lines assert on finished state: how the subscriber settled, and what reached `card-receipts`.
The body takes the started app as an argument, so the live test below runs it too.

## A payment without an amount

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/testing.rs#dropped"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#dropped"
```

</TabItem>
</Tabs>

`settled` checks the outcome the handler returned.
`assert_not_called` checks that no receipt was published.

## The handler's own commands, and a delay

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/testing.rs#held"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#held"
```

</TabItem>
</Tabs>

The handler's `INCRBY` and `DECRBY` run against the in-process Redis, as every command of the crate does.
The second payment takes the account over its limit, so it is held.
The refund is a list entry, and its handler gives the room back.

The test starts on a paused clock, and [`advance`](api:ruststream::testing::TestApp::advance) moves it forward.
A minute of `HOLD` costs nothing.
The in-process Redis reads the test's clock too, so entry ids, idle times and the scores of a delay set stand still until the clock moves.
`outcomes` lists how each delivery settled, in order; the held one is recorded as `Outcome::Nack`.

## The cap

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/testing.rs#review"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#review"
```

</TabItem>
</Tabs>

A payment above the daily limit is held on every delivery.
Each hold publishes a copy back to the stream with the retry count raised, and `published` reads every write to a name back, those copies included.
The third delivery is the last one the mount site allows, so the payment goes to `card-payments.review` as it arrived.

## What the in-process Redis models

The in-process Redis takes the commands the crate sends and answers them as Redis 8.4 does, so a [claiming](../components/streams/claiming.md) subscription mounts.
It reads every setting off the broker: the URL, the pool size, the default group and a cluster topology.
It keeps:

- key types, so a write of the wrong type fails with `WRONGTYPE`;
- consumer groups with their cursor, pending entries and delivery counts, `XAUTOCLAIM`, `XREADGROUP .. CLAIM` and a [seek](../components/positions/seek.md);
- lists, with a reliable list's processing list and its recovery set;
- Pub/Sub channels, patterns and sharded channels, which keep nothing for a subscriber that is not there;
- sorted sets, key expiry and `MULTI` / `EXEC`;
- on a cluster topology, the refusal of a command or a transaction that spans hash slots.

A handler's own commands, through the pool or a [pipelined](../components/pipelining/index.md) round, run against it too.
It runs no scripts: `EVAL`, `EVALSHA`, `FCALL` and a command it does not know fail with an error naming them.
No handshake runs, so authentication and TLS are not exercised, and neither is a failover or a resharding.
Those need a server.

## The same test against a server

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/testing.rs#live"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#live"
```

</TabItem>
</Tabs>

[`TestApp::start_live`](../components/testing/live.md) connects the same app to a running Redis.
A publish then waits on the real clock until every subscription has handled it, ten seconds at most.
The test reads the address from `REDIS_TEST_URL` with `live::url`, a helper of the example's own, and skips when it is unset.
A server keeps what earlier runs left, so the test spends from an account of its own.
The assertions are the in-process ones, unchanged.

```bash
cargo test
docker run --name redis -p 6379:6379 redis:8.4
REDIS_TEST_URL=redis://localhost:6379 cargo test -- --test-threads=1
```
