---
title: Acknowledgement
description: "What ack and nack do on each form: XACK on a stream, LREM on a reliable list, nothing on a simple list or a channel."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Acknowledgement

What ack and nack do on each form: `XACK` on a stream, `LREM` on a reliable list, nothing on a simple list or a channel.

A handler ends a delivery with an outcome: [`ack`](api:ruststream::runtime::HandlerOutcome::ack), [`retry`](api:ruststream::runtime::HandlerOutcome::retry) or [`drop`](api:ruststream::runtime::HandlerOutcome::drop).
The runtime settles the delivery with that outcome, and each form turns it into its own Redis commands.

| Form | ack | retry | drop |
| --- | --- | --- | --- |
| Stream | `XACK` | `XADD` of a copy, then `XACK` | `XACK` |
| Stream, `claiming` | `XACK` | the entry stays pending and comes back after `min_idle` | `XACK` |
| List, reliable | `LREM` off the processing list | `LPUSH` back onto the queue, then `LREM` | `LREM` |
| List, simple | nothing | nothing | nothing |
| Channel or pattern | nothing | nothing | nothing |

On a stream a retry appends a copy and then acknowledges the original, so a crash between the two leaves a duplicate rather than a loss.
The copy is a new entry with a new id.
A `claiming` subscription retries through the pending entries list instead: nothing is appended, and the entry keeps its id and its [delivery count](./counts.md).
A simple list and Pub/Sub keep nothing to settle: the entry left Redis when it was popped, and a message is gone once it is pushed.
An ack, a drop or a retry there reports [`AckError::Unsupported`](api:ruststream::AckError::Unsupported), and the runtime logs an `ack / nack failed` warning for each delivery.
A retry there delivers nothing again, unless it is a [`retry_after`](./retry-after.md) or a [cap](./dead-letter.md) is declared: both travel as a copy the runtime publishes.

## Example

A payment is charged, dropped when it is malformed, or retried when the gateway is busy.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_acknowledgement.rs#outcomes"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_acknowledgement.rs#outcomes"
```

</TabItem>
</Tabs>
