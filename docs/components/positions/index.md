---
title: RedisGroupPosition
description: "RedisGroupPosition names where a consumer group resumes: the oldest retained entry, the tail, or the entry after an id."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# RedisGroupPosition

[`RedisGroupPosition`](api:ruststream_fred::RedisGroupPosition) names where a consumer group resumes: the oldest retained entry, the tail, or the entry after an id.

| Constructor | Where the group resumes |
| --- | --- |
| `RedisGroupPosition::beginning()` | the oldest entry the stream still retains |
| `RedisGroupPosition::end()` | the tail: only entries added afterwards |
| `RedisGroupPosition::after(id)` | the entry after that [`EntryId`](./entry-id.md), which is not delivered again |

A move is `XGROUP SETID`, which rewrites the group's cursor.
Redis keeps one cursor per group, so a move applies to every consumer of that group, not only to the subscription that asked.
A subscription moves its group at startup with [`start_at`](./start-at.md), and a handler moves it while the service runs with a [seek](./seek.md).
A delivery's own position is a `RedisGroupPosition` too, and moving there delivers that entry again and the entries after it.
Streams are the form with a cursor: the core's [`Seekable`](api:ruststream::Seekable) capability is implemented for a stream subscription.
Where a group starts the first time it is created is its [`StreamStart`](../streams/stream-start.md).

## Example

A producer marks the orders queued behind a marker as void, and the worker moves its group to the tail when it reads the marker.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_positions_position.rs#position"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_positions_position.rs#position"
```

</TabItem>
</Tabs>
