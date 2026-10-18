---
sidebar_position: 10
description: "Run a Redis service on a cluster: seed nodes, hash tags for keys that work together, sharded Pub/Sub, and a test that refuses cross-slot commands."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Redis Cluster

This guide runs a service on a Redis Cluster and names its keys so that the keys a command uses together share a hash slot.

The example is a shop backend.
It runs jobs off a work queue, invoices orders off a stream, and listens to events on a sharded channel.

## Connecting

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/cluster.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/cluster.rs#app"
```

</TabItem>
</Tabs>

[`RedisBroker::cluster`](../components/broker/cluster.md) takes seed nodes.
One reachable seed is enough: the client reads the cluster's layout from it and connects to every primary.
The constructor does no I/O, like every constructor of the broker; the connection opens when the service starts.

The handlers do not change between a standalone server and a cluster.
What changes is how the service names its keys.

## Hash tags

A cluster splits keys over 16384 hash slots, and each primary owns a range of them.
A command, a `MULTI` block or a script that touches several keys runs only if all of them are on one slot.
A hash tag decides the slot: when a key has a part in braces, only that part is hashed.
`{jobs}` and `{jobs}.processing` share a slot, and so do `{orders}` and `{orders}:invoices`.

A name in braces cannot be the fixed destination of a type, because `#[outgoing(name = "..")]` reads braces as a name template.
A producer names such a key where it publishes: `.message(&job).to("{jobs}")`.

## A work queue

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/cluster.rs#jobs"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/cluster.rs#jobs"
```

</TabItem>
</Tabs>

A [reliable list](../components/lists/reliable.md) moves each entry to a processing list with one `LMOVE`, so both keys must be on one slot.
The processing list is the queue's name with `.processing` appended, and it keeps the queue's hash tag.
A reliable list whose two keys land on different slots does not start, and the error names the hash tag to use.

## Side effects on the stream's slot

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/cluster.rs#orders"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/cluster.rs#orders"
```

</TabItem>
</Tabs>

A stream is one key, so a stream subscription works under any name.
An [atomic window](pipelining.md#atomic-segments) puts the handler's commands and the `XACK` in one `MULTI` block, and the block runs on the stream's slot.
`invoice` writes to `{orders}:invoices`, on the slot of the stream `{orders}`.
A command for a key on another slot makes Redis refuse the whole block: nothing is written and the entry stays pending.

A [transaction](transactions.md) of the stream publisher is a `MULTI` block too, and its destinations are not tied to one slot.
So a service that mounts `TransactionalPublish` does not start on a cluster.

## Sharded Pub/Sub

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/cluster.rs#events"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/cluster.rs#events"
```

</TabItem>
</Tabs>

Classic Pub/Sub on a cluster sends every message to every node.
[`PubSubMode::Sharded`](../components/pubsub/pubsub-mode.md) sends it only to the node that owns the channel's slot, with `SPUBLISH` and `SSUBSCRIBE`, so the traffic scales with the cluster.
It needs Redis 7 or later.

The two modes do not reach each other: a sharded message never reaches a classic subscriber, and the other way round.
A publisher of the channel uses the subscription's mode.
A [pattern subscription](../components/pubsub/pattern.md) is classic only.

## Testing a cluster service

The tests start the production app under `TestApp::start`, with the cluster broker.
The [in-process Redis](../components/testing/index.md) behaves like a cluster: it refuses a command or a block that spans hash slots.
So the tests check that every form runs with the service's hash tags, that a reliable list without one does not start, and that a block across two slots commits nothing.
The same test body runs against a real cluster when `REDIS_CLUSTER_TEST_URL` names a node.

A failover or a resharding happens only on a real cluster; the [production guide](production.md) covers what the broker does then.
