---
sidebar_position: 1
title: atomic
description: ".atomic() after .pipeline() wraps each delivery's commands and settle in MULTI and EXEC, so they apply together or not at all."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# atomic

[`.atomic()`](api:ruststream_fred::pipeline::RedisPipelineSteps::atomic) after `.pipeline()` wraps each delivery's commands and settle in `MULTI` and `EXEC`, so they apply together or not at all.

The step follows `.pipeline()` at the mount site: `b.include(handler.pipeline().atomic())`.
A delivery's segment becomes `MULTI`, the commands its handler queued, the delivery's [settle](../acknowledgement/index.md) where the form has one, and `EXEC`.
The transaction still travels inside the window's pipeline, so it costs no round trip of its own.
A handler's side effects and its acknowledgement are then one unit: a reader never sees the effects of an entry that is still pending.
`.atomic()` without `.pipeline()` does not compile, and the error says to write `.pipeline().atomic()`.

The transaction is Redis's, with Redis's limits.
A command that fails inside `EXEC`, a `WRONGTYPE` for example, leaves the other commands of the block executed: Redis does not roll back.
On a [cluster](../broker/cluster.md), a transaction runs only when every key in it hashes to one slot, the slot of the subscription's key.
A hash tag gives the handler's keys that slot: `{orders}:invoices` beside a stream `{orders}`.
A command for another slot makes Redis refuse the whole `EXEC`, so a segment never runs in part.

## Example

An invoice is queued beside each paid order and leaves in one `MULTI` / `EXEC` with the order's `XACK`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_pipelining_atomic.rs#atomic"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_pipelining_atomic.rs#atomic"
```

</TabItem>
</Tabs>
