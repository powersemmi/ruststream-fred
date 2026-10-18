---
sidebar_position: 2
title: claiming
description: "claiming reads pending entries idle at least min_idle and the fresh tail in one XREADGROUP CLAIM call; it needs Redis 8.4."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# claiming

[`claiming`](api:ruststream_fred::stream::RedisStream::claiming) reads pending entries idle at least `min_idle` and the fresh tail in one `XREADGROUP ... CLAIM` call; it needs Redis 8.4.

The read takes the stale entries first, the longest idle first, and fills the rest with fresh ones.
One subscription and one handler cover new work and recovery alike.
`min_idle` has no default.
Set it above the longest time a handler takes, or an entry a healthy consumer is still working on is taken over and handled twice.
A [retry](../acknowledgement/index.md) leaves the entry in the pending entries list: nothing is appended, and the entry keeps its id.
The same subscription reads it again once it has been idle `min_idle`.
So a retry waits out `min_idle`, and a [`retry_after`](../acknowledgement/retry-after.md) shorter than that waits `min_idle` too.
Each delivery reports the [attempts](../acknowledgement/counts.md) before it in the [`DELIVERY_COUNT_HEADER`](api:ruststream_fred::DELIVERY_COUNT_HEADER) header, zero for an entry read off the tail, and its idle time in [`IDLE_MS_HEADER`](api:ruststream_fred::IDLE_MS_HEADER).
A server older than 8.4 refuses the subscription, and the service does not start.
The error, [`RedisError::ServerTooOld`](api:ruststream_fred::RedisError::ServerTooOld), names the subscription, the mode and both versions.
On such a server the recovery is a [`reclaim`](./reclaim.md) subscription beside a `new` one.

## Example

An order out of stock stays pending and comes back after thirty seconds.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_streams_claiming.rs#claiming"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_streams_claiming.rs#claiming"
```

</TabItem>
</Tabs>
