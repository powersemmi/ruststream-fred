---
sidebar_position: 3
description: "Share a Redis stream between workers and services with consumer groups, and recover the entries a stopped worker never acknowledged."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Consumer groups and crash recovery

This guide shares a Redis stream between workers and between services, and recovers the entries a stopped worker left unfinished.

The example is a payments service.
Payments arrive on the stream `payments`.
The ledger group posts each payment, and the fraud group screens it.

## A group of workers

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/consumer_groups.rs#ledger"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/consumer_groups.rs#ledger"
```

</TabItem>
</Tabs>

A Redis stream is read through a consumer group.
The group holds a cursor into the stream, and every entry past the cursor goes to one consumer of the group.
Each subscription is one consumer, so every running instance of the service adds a consumer to `ledger`, and the instances share the payments.
A consumer gets a generated name, `ruststream-0`, `ruststream-1` and so on in each process.
[`consumer`](api:ruststream_fred::stream::RedisStream::consumer) names it, for example after the host, so `XINFO CONSUMERS` tells the instances apart.

An entry handed to a consumer goes into the group's pending entries list.
It leaves the list when the handler acknowledges it.
If the worker stops first, a crash or a deploy, the entry stays pending: Redis keeps it, and no other consumer receives it on its own.

[`RedisStream::claiming`](../components/streams/claiming.md) reads both sets in one call: entries pending for at least `min_idle`, then new ones.
So the subscription that takes new payments also takes over the payments of a worker that stopped.
It needs Redis 8.4 or later; on an older server the service does not start, and the error names the subscription and both versions.

A `retry` on this subscription does not append a copy.
The entry stays pending under its own id, and the next read claims it once it has been idle `min_idle`.
`post_payment` uses that for an account that is being migrated: the payment waits in the pending list and comes back every 30 seconds.

A claimed entry carries [`DELIVERY_COUNT_HEADER`](../components/acknowledgement/counts.md): the number of deliveries before this one, zero for a new entry.
[`IDLE_MS_HEADER`](api:ruststream_fred::IDLE_MS_HEADER) carries how long the entry sat idle.
[Retries, delays and dead letters](retries.md) caps the deliveries on this count.

## Choosing min_idle

`min_idle` has no default.
An entry idle that long is treated as abandoned, so set it above the longest time a handler ever runs.
A shorter value takes an entry away from a worker that is still on it, and the payment is posted twice.
A longer value delays recovery after a crash by the same amount.

## A second group

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/consumer_groups.rs#fraud"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/consumer_groups.rs#fraud"
```

</TabItem>
</Tabs>

Each group has its own cursor and its own pending list, so each group reads every entry.
The fraud group receives every payment the ledger group receives, and neither slows the other.
In practice each service that reads a stream has a group of its own.

The service creates a group at startup when it does not exist yet.
[`start_id`](../components/streams/stream-start.md) says where a new group starts: [`StreamStart::New`](api:ruststream_fred::stream::StreamStart::New), the default, reads only entries added after that, and [`StreamStart::Beginning`](api:ruststream_fred::stream::StreamStart::Beginning) reads every entry the stream still holds.
The fraud group was added after the ledger, so it starts at the beginning and screens the payments already on the stream.
An existing group keeps its cursor, and `start_id` does not move it; [start_at](../components/positions/start-at.md) does.

## The service

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/consumer_groups.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/consumer_groups.rs#app"
```

</TabItem>
</Tabs>

`screen_payment` replies with a `Screened` on the stream `payments.screened`.

## Order per account

`workers(8, by_key)` runs `post_payment` on eight lanes.
Payments that share a partition key go to the same lane, one after another, so the payments of one account are posted in the order they were added.
Redis has no partitions, so the key travels in the [`PARTITION_KEY_HEADER`](api:ruststream_fred::PARTITION_KEY_HEADER) header.
The producer sets it with the [`partition_key`](../components/publishing/partition-key.md) step of its publish.
A payment without a key goes to any lane.
The order holds inside one consumer; two instances of the service each post their own share of the payments.

## Recovery before Redis 8.4

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/consumer_groups.rs#legacy"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/consumer_groups.rs#legacy"
```

</TabItem>
</Tabs>

An older server has no claiming read, so recovery is a second subscription on the same group.
[`RedisStream::new`](../components/streams/index.md) reads new entries.
[`RedisStream::reclaim`](../components/streams/reclaim.md) reads only entries pending for at least `min_idle`, with `XAUTOCLAIM`, so it takes over what a stopped worker held.
A reclaimed delivery counts itself in `DELIVERY_COUNT_HEADER`, so the first recovery reports at least 1.

On a `new` subscription, `retry` appends a copy of the entry to the stream and acknowledges the original.
The copy comes back at once, behind the entries added meanwhile.
A wait such as the migration above takes `retry_after` there, which [Retries, delays and dead letters](retries.md) covers.

## Testing recovery

The tests in the example file run the service in process on a paused clock.
One publishes a payment for an account under migration, moves the clock by `min_idle` with `tb.advance`, and sees the ledger receive the payment a second time, with no copy on the stream.
[Testing a Redis service](testing.md) covers the harness.
