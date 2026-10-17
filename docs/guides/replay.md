---
sidebar_position: 9
description: "Replay a Redis stream from its oldest entry on every start, skip a consumer group to the tail from a handler, and know what a seek leaves alone."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Replay and seeking a stream

This guide moves a consumer group's cursor on a Redis stream: back to replay entries, forward to skip them.

The example is a payments service.
It rebuilds a view of balances from the ledger stream, and sends alerts to customers from another stream.

## Replay from the beginning

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/replay.rs#rebuild"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/replay.rs#rebuild"
```

</TabItem>
</Tabs>

A Redis stream keeps its entries after they are read and acknowledged.
A consumer group keeps one cursor over them: the id of the last entry it was given.
Replaying means moving that cursor back, which by hand is `XGROUP SETID`.

[`start_at`](../components/positions/start-at.md) moves the cursor before the subscription's first delivery, on every start.
[`RedisGroupPosition::beginning`](../components/positions/index.md) is the oldest entry the stream still retains, so `rebuild` reads the whole ledger each time the service starts.
A view rebuilt this way starts from an empty state, such as one held in memory.

The cursor belongs to the group, not to the subscription.
Every consumer of `balances` replays, in this service and in every other instance of it.

## The mount

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/replay.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/replay.rs#app"
```

</TabItem>
</Tabs>

With the macros, `start_at(..)` is a clause of `#[subscriber]`.
In the manual form it is a step of the mount, after `build()`.

| Position | Where the group resumes |
| --- | --- |
| `RedisGroupPosition::beginning()` | the oldest entry the stream retains |
| `RedisGroupPosition::end()` | the tail: only entries added afterwards |
| `RedisGroupPosition::after(id)` | the entry after `id`, which itself is skipped |

[`StreamStart`](../components/streams/stream-start.md) is a different setting: it says where a group starts when it does not exist yet.
`start_at` moves a group that exists, on every start.

## Skip to the tail

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/replay.rs#skip"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/replay.rs#skip"
```

</TabItem>
</Tabs>

A handler moves its group while the service runs through the [seek handle](../components/positions/seek.md).
`Ctx<keys::SeekHandle>` gives it; its `group()` names the group it moves.

A producer floods the `alerts` stream by mistake, and an operator appends a halt marker behind the flood.
`notify` reads the marker and moves the group to the tail, so the alerts queued behind it are never sent.
The seek takes effect as soon as it returns.
Entries the old cursor had already selected are discarded, not delivered, and a subscription blocked in `XREADGROUP` reads under the new cursor within one `block` interval.

A batch handler reads the same handle with `ctx.context(keys::SeekHandle)` on its batch context.

## Replay one delivery

A delivery knows its own position: [`Ctx<keys::Position>`](../components/positions/seek.md) is the cursor that redelivers it.
A seek to it delivers the same entry again, then the entries after it, in stream order.
A handler that must keep order on a transient failure can seek there instead of returning `retry`, because on a stream `retry` appends a copy at the tail.
[`Ctx<keys::EntryId>`](../components/positions/entry-id.md) is the entry's own id, and `RedisGroupPosition::after(id)` builds a position from an id stored elsewhere.

## What a seek leaves alone

A seek moves the cursor and nothing else.

- An entry delivered and not acknowledged stays in the group's pending entries list, and a [reclaim](../components/streams/reclaim.md) or [claiming](../components/streams/claiming.md) subscription still finds it.
- A copy waiting in a [delay](../components/acknowledgement/delayed-retry.md) set is appended when it falls due, wherever the group reads.
- A replayed entry is delivered again, so Redis's delivery count grows; a claiming subscription counts replays towards its cap, while the framework's own retry count does not move.

## Retention

Redis keeps a stream's entries until something trims them, and a replay can only reach what is kept.
Trimming with `XTRIM` is the service's decision: a handler sends it through [`Ctx<keys::FredPool>`](../components/context/fred-pool.md), or an operator runs it.
A group whose cursor points before the oldest retained entry reads from the oldest one.

## Testing

The tests in the example files run in process.
They check that the view reads every entry and that a halt marker skips the alerts queued behind it.
A replay across a restart needs a server that keeps the stream between two starts, so that test runs live, against `REDIS_TEST_URL`.
