---
sidebar_position: 2
title: DelayedRetry
description: "DelayedRetry::DurableZset parks a delayed delivery in a sorted set, so the retry survives a crash and fires at its due time."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# DelayedRetry

[`DelayedRetry::DurableZset`](api:ruststream_fred::DelayedRetry::DurableZset) parks a delayed delivery in a sorted set, so the retry survives a crash and fires at its due time.

A stream subscription takes it with [`RedisStream::delayed_retry`](api:ruststream_fred::stream::RedisStream::delayed_retry).
A [`retry_after`](./retry-after.md) then adds the entry to the sorted set with `ZADD`, scored by its due time, with its retry count raised by one.
The original is acknowledged with `XACK` once the entry is parked.
Each read of the subscription sweeps the set and appends the entries that are due back onto the stream, each under a new id.
Nothing about the delay lives in the service, so a restart or a crash loses no retry.
The sweep runs once per read, so the `block` interval sets how late a due entry may come back, and one pass moves at most 128 entries.
Scores are wall-clock epoch milliseconds, so the clocks of the services that share a set have to agree.
`ttl` expires a set no retry has refreshed for that long, and it has to exceed the longest delay, or entries are dropped before they are due.
The set costs keys, memory and a sweep, so a subscription has it only when its descriptor names it.
On a `claiming` subscription the set honours the delay as asked, instead of rounding it to `min_idle`.

## Example

A bank payment the gateway timed out on waits in `bank-payments.delayed` for 30 seconds.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_acknowledgement_retries.rs#delayed_retry"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_acknowledgement_retries.rs#delayed_retry"
```

</TabItem>
</Tabs>
