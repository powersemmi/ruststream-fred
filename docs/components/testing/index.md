---
title: Testing
description: "TestApp::start runs the service's app on RedisBroker connected to a Redis modelled in process, groups, lists and channels included."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Testing

[`TestApp::start`](api:ruststream::testing::TestApp::start) runs the service's app on [`RedisBroker`](../broker/index.md) connected to a Redis modelled in process, groups, lists and channels included.

A test hands the harness the app `main` runs, unchanged, and addresses the broker by its production type, `tb.broker::<RedisBroker>()`.
The `testing` feature in `[dev-dependencies]` adds the in-process mode, and turns on the core's harness with it.
The connected broker is the production one.
Every command it sends, a publish, a group read, an acknowledgement, a delay-queue sweep, reaches the model as that command and is answered as Redis answers it.

The model reads every setting off the broker.
Its address is parsed as the connect parses it, and the [pool size](../broker/pool.md), the [default group](../broker/default-group.md) and the [cluster](../broker/cluster.md) topology carry over.
It answers as Redis 8.4, so a [`claiming`](../streams/claiming.md) subscription mounts.
It keeps key types: a write or a read of the wrong type fails with `WRONGTYPE`.
It keeps consumer groups with their cursor, pending entries and delivery counts, the group's [`start_id`](../streams/stream-start.md), `XAUTOCLAIM` and `XREADGROUP ... CLAIM` at their idle thresholds, and a [seek](../positions/seek.md).
It keeps lists with the processing list and its recovery set, sorted sets, key expiry and `MULTI` / `EXEC`.
Pub/Sub channels, globs and sharded channels deliver to the subscriptions open at that moment and store nothing for one that is not.
On a cluster topology it refuses a command or a transaction that spans hash slots.
It runs no scripts: `EVAL`, `EVALSHA`, `FCALL` and a command it does not know fail with an error naming them.
No handshake runs, so `AUTH`, `HELLO` and TLS are for a [live test](./live.md).

A test's input goes where the default publisher writes its name: an `XADD`, or the `LPUSH` or `PUBLISH` of the subscription that reads the name.
A publish through the harness returns once every handler it woke has settled, a reply included, so the assertions that follow read finished state.
`published::<T>(name)` reads every write to the name back, a requeued copy and an entry a delay queue adds back included.
The model's time is the test's clock: entry ids, idle times and the scores of the delay queue and the recovery set stand still on a paused clock, and `tb.advance(by)` moves them.
A handler's own commands, through [`FredPool`](../context/fred-pool.md) or a [pipeline](../pipelining/index.md) window, reach the model too.

## Example

The dependency line:

```toml
[dev-dependencies]
ruststream-fred = { version = "0.7", features = ["testing"] }
```

The first test publishes a payment and reads how the handler settled it.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_testing.rs#testapp"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_testing.rs#testapp"
```

</TabItem>
</Tabs>

A settlement is answered with a receipt on the stream `receipts`, and the test reads the receipt back.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_testing.rs#published"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_testing.rs#published"
```

</TabItem>
</Tabs>

The model's time is the test's clock.
Entry ids, idle times and the scores of a [delay queue](../acknowledgement/delayed-retry.md) stand still on a paused clock, and `tb.advance(by)` moves them.
A declined invoice comes back once the clock has moved past its delay.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_testing.rs#clock"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_testing.rs#clock"
```

</TabItem>
</Tabs>
