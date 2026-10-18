---
sidebar_position: 3
title: StreamBatchContext
description: "A stream batch handler's context carries only the consumer group and the seek handle, which belong to the whole subscription."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# StreamBatchContext

A stream batch handler's context, [`StreamBatchContext`](api:ruststream_fred::context::StreamBatchContext), carries only the consumer group and the seek handle, which belong to the whole subscription.

A batch spans many entries, so a field that differs per entry has no single value for it.
The body reads the batch's fields with `ctx.context(key)`: [`keys::ConsumerGroup`](./consumer-group.md), [`keys::SeekHandle`](api:ruststream_fred::context::keys::SeekHandle) and [`keys::FredPool`](./fred-pool.md).
A `Ctx<K>` parameter binds the per-delivery context, so a batch body reads its fields off the context instead.
The handler names the context in its signature, `ctx: &mut Context<'_, StreamBatchContext>`, beside a slice payload.
The batch itself is the read's `COUNT`: `.batch(n)` at the mount site asks Redis for up to `n` entries per `XREADGROUP`.

## Example

The batch is billed in one call, and every invoice records the group, read once off the batch's context.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_context_batch.rs#batch"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_context_batch.rs#batch"
```

</TabItem>
</Tabs>
