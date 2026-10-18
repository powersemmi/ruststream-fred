---
sidebar_position: 2
description: "Choose between a Redis stream, a list and a Pub/Sub channel: what each keeps, who receives a message, and what acknowledgement does."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Choosing a transport

This guide compares the three Redis structures a service reads: a stream, a list and a Pub/Sub channel.

The example is a shop service that uses all three.
Orders are billed off a stream, thumbnails are rendered off a list, and price changes arrive on a channel.

## Streams: a durable log

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/choosing_a_transport.rs#stream"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/choosing_a_transport.rs#stream"
```

</TabItem>
</Tabs>

A Redis stream keeps every entry until the service trims it.
[`RedisStream`](../components/streams/index.md) reads it through a consumer group.
Each group receives every entry once, and inside a group one consumer handles it.
An acknowledged entry is `XACK`ed.
An entry a consumer never acknowledged stays in the group's pending entries list, so a crash loses nothing.
A stream can be read again from an earlier entry with a [seek](../components/positions/seek.md).

Orders are money, so they go on a stream.
`bill` answers each order with an `Invoice`, which lands on the stream `invoices`.

## Lists: a work queue

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/choosing_a_transport.rs#list"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/choosing_a_transport.rs#list"
```

</TabItem>
</Tabs>

A Redis list is a queue: a producer pushes an entry, and exactly one consumer pops it.
[`RedisList::new`](../components/lists/index.md) pops with `BRPOP`, so the entry leaves Redis before the handler runs, and a crash in the handler loses it.
[`reliable`](../components/lists/reliable.md) moves the entry to a processing list instead and removes it only once the handler acknowledges it.
A list has no groups and no replay.

Rendering a thumbnail is work for one worker, so the jobs go on a reliable list.
`render` announces each finished thumbnail on the channel `thumbnails.ready`.

## Pub/Sub: a broadcast

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/choosing_a_transport.rs#pubsub"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/choosing_a_transport.rs#pubsub"
```

</TabItem>
</Tabs>

A Pub/Sub message reaches every subscriber connected at the moment it is published.
Redis keeps nothing: a subscriber that is not connected never sees the message.
[`RedisPubSub`](../components/pubsub/index.md) has no acknowledgement, so the outcome a handler returns settles nothing.

A price change matters only to the instances running now, which refresh their cache, so it goes on a channel.

## Side by side

| | Stream | List | Pub/Sub |
|---|---|---|---|
| Kept while nobody reads | until trimmed | until popped | no |
| Who receives a message | one consumer in every group | one consumer | every connected subscriber |
| `ack` | `XACK` | reliable: `LREM` off the processing list; simple: nothing | nothing |
| `retry` | a copy appended to the stream | reliable: back to the list; simple: lost | lost |
| `retry_after` | a copy after the delay | a copy after the delay | a copy after the delay |
| A batch | one read of up to `n` entries | assembled by the client | assembled by the client |
| Replay | a seek of the group | none | none |

The [acknowledgement](../components/acknowledgement/index.md) pages give each outcome in full, and [Retries, delays and dead letters](retries.md) shows how a delay and a cap work on each structure.
A [batch](../components/subscriptions/batches.md) on a stream is the `COUNT` of one read.
A list and a channel deliver one entry at a time, so the client collects a batch of the same size.

A name holds one structure.
A stream and a list are keys of a type, and a write of the other type fails with `WRONGTYPE`.
A channel is no key at all: an `XADD` under a channel's name writes a stream its subscribers never see.

## One service, three transports

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/choosing_a_transport.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/choosing_a_transport.rs#app"
```

</TabItem>
</Tabs>

A service on one transport imports that transport's prelude, such as `ruststream_fred::stream::prelude`.
This one mixes transports, so it imports [`ruststream_fred::prelude`](../components/broker/prelude.md).
There each transport's publish policy keeps its module's name: `stream::Publish` writes with `XADD`, `pubsub::Publish` with `PUBLISH`.
A reply type names where the reply goes, and the policy at the mount site names how it gets there.

The three handlers have the same shape.
Moving a handler to another transport changes the descriptor in its attribute and the policy at its mount site, not its body.

The tests in the example file publish to each transport and read what the handlers sent back, all in process.
