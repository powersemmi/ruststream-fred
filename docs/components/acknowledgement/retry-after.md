---
sidebar_position: 1
title: retry_after
description: "retry_after on a plain stream makes the runtime republish a copy after the delay, which a crash before the timer loses."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# retry_after

[`retry_after(delay)`](api:ruststream::runtime::HandlerOutcome::retry_after) on a plain stream makes the runtime republish a copy after the delay, which a crash before the timer loses.

Redis has no per-message delay, so the runtime serves it.
It settles the delivery, holds the payload and headers in memory, and publishes a copy once the delay is up.
The copy goes where the subscription's descriptor says a copy goes: the stream key, the list key or the channel, and for a pattern the channel its registration names.
It carries [`RETRY_COUNT_HEADER`](api:ruststream::runtime::RETRY_COUNT_HEADER) one higher than the delivery it replaces.
The copy waits in the service's memory, so it is at most once over the delay: a process that exits before the timer fires loses it.
A simple list and Pub/Sub settle nothing, and they get the copy all the same.
The copy leaves through the registration's retry publisher, which [`out_retry`](./out-retry.md) replaces.
A stream that has to keep the delayed message across a crash parks it in Redis with [`DelayedRetry`](./delayed-retry.md).
A `claiming` subscription keeps the entry pending instead, so the delay rounds up to its `min_idle`, and a longer delay is not held that long.

## Example

A card payment the gateway timed out on is charged again 30 seconds later.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_acknowledgement_retries.rs#retry_after"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_acknowledgement_retries.rs#retry_after"
```

</TabItem>
</Tabs>
