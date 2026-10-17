---
sidebar_position: 5
title: Flush
description: "A window flushes when the read's COUNT has settled, when nothing is outstanding, or when the subscription stops."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Flush

A window flushes when the read's `COUNT` has settled, when nothing is outstanding, or when the subscription stops.

The flush sends the segments of the acknowledged deliveries, then one pipeline of settles, on a connection of the pool apart from the one the reads block on.
A stream's settles leave as one `XACK`, a [reliable list](../lists/reliable.md)'s as one `LREM` per entry.
Under load that is one round trip per fetched batch; on a trickle the window leaves as soon as the last handler of the read returns.
What a handler queued follows its delivery's outcome:

| Outcome | The handler's commands | The settle |
| --- | --- | --- |
| `ack` | sent, before the settle | the form's own: `XACK`, `LREM` |
| `drop` | dropped | sent alone |
| `retry`, [`retry_after`](../acknowledgement/retry-after.md) | dropped | the retry's: the copy or the `ZADD`, then `XACK` |
| unsettled | dropped | none: the entry stays pending |

[Pub/Sub](../pubsub/index.md) and a [simple list](../lists/index.md) settle nothing, so on them every outcome but `ack` drops the segment.
A flush that fails is reported once on the delivery stream, with the subscription, the number of commands and settles it carried, and how many of them failed.
On the forms that settle, an entry whose settle failed stays pending and is delivered again.
The [Window](../../architecture/window.md) page draws the flush and the [Settlement](../../architecture/settlement.md) page what each outcome becomes per form.

A [batch](../subscriptions/batches.md) mount's segment is the whole batch.
The body's commands and the settles of every entry leave together, and the commands only if every entry is acknowledged.
A pipelined reliable list reads its batch with one pipeline of `LMOVE`, a simple one with `RPOP key count`.

## Example

A batch handler records every order of the batch; one order without items drops the batch, and every push it queued.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_pipelining_flush.rs#batch"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_pipelining_flush.rs#batch"
```

</TabItem>
</Tabs>
