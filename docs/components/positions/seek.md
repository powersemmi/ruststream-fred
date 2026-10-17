---
sidebar_position: 2
title: Seek
description: "A handler moves its group's cursor through the SeekHandle key; a delivery's Position key seeks back to that delivery."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Seek

A handler moves its group's cursor through the [`SeekHandle`](api:ruststream_fred::context::keys::SeekHandle) key; a delivery's [`Position`](api:ruststream_fred::context::keys::Position) key seeks back to that delivery.

`Ctx<keys::SeekHandle>` binds a [`RedisGroupSeeker`](api:ruststream_fred::RedisGroupSeeker) for the group the delivery came from.
A batch body reads the same key off its [`StreamBatchContext`](api:ruststream_fred::context::StreamBatchContext) with `ctx.context(keys::SeekHandle)`.
`seek(position)` takes a [`RedisGroupPosition`](./index.md) and moves the cursor of the whole group, every consumer of it included.
The cursor moves as soon as `seek` returns.
A read blocked in `XREADGROUP` sees the move within one [`block`](../subscriptions/block.md), and the entries it had selected under the old cursor are discarded, never delivered.
`Ctx<keys::Position>` binds the position of the delivery itself; a seek there delivers that entry again and the entries after it.
The group's cursor is exclusive, so that position holds the id one below the entry's, and the crate computes it.
A seek leaves three things alone.
Entries delivered and not yet acknowledged stay in the pending entries list, where a [`reclaim`](../streams/reclaim.md) subscription can take them.
Copies waiting in a delay queue are appended when they fall due, wherever the group reads.
A replayed entry is delivered as a new one: Redis starts its delivery count in the pending entries list over at one.
So a cap that reads that count, on a [`claiming`](../streams/claiming.md) or a `reclaim` subscription, counts from the replay, and the retry-count header the entry carries stays as it was.
Seeking is a stream's: a list and a Pub/Sub channel hand each value out once and keep no read position.
A seek after the service shut down returns an error.
A position that has to hold on every start belongs in [`start_at`](./start-at.md).

## Example

A batch that holds the void marker moves the group to the tail once it is billed, so the orders queued behind the marker are never billed.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_positions_seek.rs#seek"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_positions_seek.rs#seek"
```

</TabItem>
</Tabs>
