---
title: StreamContext
description: "StreamContext gives a stream handler the entry id, the position that redelivers it, the consumer group and the group's seek handle."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# StreamContext

[`StreamContext`](api:ruststream_fred::context::StreamContext) gives a stream handler the entry id, the position that redelivers it, the consumer group and the group's seek handle.

The subscription builds it once per delivery, beside the payload and the headers.
A handler names it as its context type, `ctx: &mut Context<'_, StreamContext>`, and reads a field with `ctx.context(key)`.
A handler that needs one field binds it as a parameter with [`Ctx<K>`](api:ruststream::runtime::Ctx) instead.
Each key is a type in [`context::keys`](api:ruststream_fred::context::keys), and the stream prelude brings the module in as `keys`:

| Key | Value | What it is |
| --- | --- | --- |
| [`EntryId`](api:ruststream_fred::context::keys::EntryId) | [`EntryId`](api:ruststream_fred::EntryId) | the `<milliseconds>-<sequence>` id the entry was read at; it orders the way the stream does |
| [`Position`](api:ruststream_fred::context::keys::Position) | [`RedisGroupPosition`](api:ruststream_fred::RedisGroupPosition) | the group cursor that delivers this entry again |
| [`ConsumerGroup`](api:ruststream_fred::context::keys::ConsumerGroup) | `&str` | the group the subscription reads through, as [ConsumerGroup](./consumer-group.md) shows |
| [`SeekHandle`](api:ruststream_fred::context::keys::SeekHandle) | [`RedisGroupSeeker`](api:ruststream_fred::RedisGroupSeeker) | the handle that moves the whole group to another position |
| [`FredPool`](api:ruststream_fred::context::keys::FredPool) | `fred::clients::Pool` | the broker's connection pool, as [FredPool](./fred-pool.md) shows |

A key belongs to the context types that carry its field, so a key read off the wrong form's context does not compile.
A batch handler reads [`StreamBatchContext`](./batch-context.md), a Pub/Sub handler [`PubSubContext`](./pubsub-context.md).
A list delivery carries its payload and headers alone; a list handler reads the pool through `FredPool` or keeps the default `()` context.
The two claiming read modes report a delivery's count and idle time as the [`DELIVERY_COUNT_HEADER`](api:ruststream_fred::DELIVERY_COUNT_HEADER) and [`IDLE_MS_HEADER`](api:ruststream_fred::IDLE_MS_HEADER) headers, which every transport reads the same way.
A context costs a handler that does not name one nothing: the default `()` context builds no value.

## Example

The handler writes where each order sat in the stream, and which group read it, to an order trail.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_context_stream.rs#context"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_context_stream.rs#context"
```

</TabItem>
</Tabs>
