---
sidebar_position: 1
title: reclaim
description: "reclaim reads only entries another consumer of the group fetched and never acked, idle at least min_idle, with XAUTOCLAIM."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# reclaim

[`reclaim`](api:ruststream_fred::stream::RedisStream::reclaim) reads only entries another consumer of the group fetched and never acked, idle at least `min_idle`, with `XAUTOCLAIM`.

It is the recovery path for a consumer that crashed or hung while it held entries.
It runs as a second subscription on the same group, beside a [`RedisStream::new`](./index.md) one that reads the fresh tail.
A fresh entry is no one's unfinished work, so a reclaim subscription never reads it.
`min_idle` has no default.
Set it above the longest time a handler takes, or an entry a healthy consumer is still working on is taken over and handled twice.
The subscription scans the pending entries list, and between two scans that found nothing it waits its [`block`](../subscriptions/block.md).
A reclaimed delivery carries the server's [count of its deliveries](../acknowledgement/counts.md), this one included, in the [`DELIVERY_COUNT_HEADER`](api:ruststream_fred::DELIVERY_COUNT_HEADER) header.
A [retry](../acknowledgement/index.md) on a reclaim subscription appends a copy of the entry to the stream and acknowledges the original.
On Redis 8.4 and later one [`claiming`](./claiming.md) subscription covers both the fresh tail and the recovery.

## Example

A worker reads the fresh tail of `orders`, and a recovery handler of the same group takes back what a worker left pending for a minute.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_streams_reclaim.rs#reclaim"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_streams_reclaim.rs#reclaim"
```

</TabItem>
</Tabs>
