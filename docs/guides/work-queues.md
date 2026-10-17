---
sidebar_position: 4
description: "Run a work queue on a Redis list: a simple queue, a reliable one with a processing list, recovery of a stopped worker's job, and expiry."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Work queues on lists

This guide runs a work queue on a Redis list, from a simple queue to a reliable one that recovers the jobs of a stopped worker.

The example is an image service.
It warms its page cache from one queue and renders thumbnails from another.

## A simple queue

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/work_queues.rs#simple"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/work_queues.rs#simple"
```

</TabItem>
</Tabs>

A producer pushes an entry onto the left of a list with `LPUSH`.
[`RedisList::new`](../components/lists/index.md) pops from the right with `BRPOP`, so entries come out in the order they went in, and each one reaches exactly one consumer.
Every running instance of the service pops from the same list, so the instances share the work.

The pop removes the entry from Redis before the handler runs.
If the service stops while the handler runs, the entry is gone.
The queue delivers each entry at most once, and the handler's outcome settles nothing.
A missed cache warmup costs one slow page, so `cache.warmups` is a simple queue.

## A reliable queue

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/work_queues.rs#reliable"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/work_queues.rs#reliable"
```

</TabItem>
</Tabs>

[`reliable`](../components/lists/reliable.md) moves each entry to a processing list in one command, `BLMOVE`, instead of popping it.
The processing list is `{thumbnails}.processing`, the queue's key with `.processing` after it, unless [`processing`](../components/lists/processing.md) names another.
The entry stays there while the handler runs:

- `ack` removes it from the processing list with `LREM`;
- `retry` moves it back to the queue;
- `drop` removes it.

So a job leaves Redis only once a handler has finished with it, and the queue delivers each entry at least once.

## Recovering a stopped worker's job

A worker that stops between the move and the acknowledgement leaves its entry on the processing list.
A list keeps no record of who took an entry or when, so nothing returns it on its own.

[`recovery_zset`](../components/lists/recovery.md) turns on a watchdog for that.
Each move records the entry in a sorted set, scored by the time it was taken, and the acknowledgement removes the record.
As the subscription reads, it moves every entry idle longer than `min_idle` back onto the queue, where any worker can take it.
`min_idle` is required with the recovery set, and a subscription without it does not start.
Set it above the longest time a job ever takes: a shorter value takes a job away from a worker that is still on it, and the thumbnail is rendered twice.
[`recovery_ttl`](api:ruststream_fred::list::RedisList::recovery_ttl) expires an abandoned recovery set, and has to be longer than `min_idle`.

The scores are wall-clock milliseconds, so the clocks of the service's hosts need to agree.

## Feeding the queue

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/work_queues.rs#producer"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/work_queues.rs#producer"
```

</TabItem>
</Tabs>

`queue_thumbnail` reads uploads off a stream and answers each one with a thumbnail job.
The job goes to `{thumbnails}`, which the mount site names.
A name in `#[outgoing(name = ..)]` reads braces as a name template, so a key with a hash tag is named at the mount site instead.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/work_queues.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/work_queues.rs#app"
```

</TabItem>
</Tabs>

The reply leaves through [`list::Publish`](../components/publishing/list-publish.md), which pushes with `LPUSH`.
Its [`ttl`](api:ruststream_fred::list::RedisListPublish::ttl) re-arms an expiry on the queue's key at every push.
A queue in use never expires, and a queue nobody fed for a day lapses with whatever it still holds.
A list has no per-entry expiry, so the expiry always covers the whole queue.

## Queues on a cluster

On a [cluster](../components/broker/cluster.md), a reliable queue and its processing list have to live on one hash slot, because one command moves an entry between them.
A hash tag gives them one: Redis hashes only the part of the key inside the braces, so `{thumbnails}` and `{thumbnails}.processing` land on the same node.
A reliable queue whose two keys land on different slots does not start, and the error names the hash tag to use.
The example names its keys with a tag already, so it moves to a cluster unchanged.

## When to use a stream instead

A list keeps no history: an entry that was handled is gone, and no second service can read it too.
The recovery watchdog is the only redelivery a list has, and a list has no delivery count of its own.
When several services read the same work, when the work needs replay, or when a stopped worker's job must come back without a watchdog, use a stream with a consumer group, as [Consumer groups and crash recovery](consumer-groups.md) shows.

The tests in the example file publish a warmup and an upload and check what the handlers received.
One moves the clock past `min_idle` and checks that a rendered job does not come back.
