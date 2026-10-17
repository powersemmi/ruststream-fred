---
sidebar_position: 2
title: Recovery
description: "recovery_zset starts a watchdog that returns entries a dead consumer left on its processing list, once idle min_idle."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Recovery

[`recovery_zset`](api:ruststream_fred::list::RedisList::recovery_zset) starts a watchdog that returns entries a dead consumer left on its processing list, once idle [`min_idle`](api:ruststream_fred::list::RedisList::min_idle).

A reliable list keeps no idle time of its own, so the subscription records every claim in the named sorted set, scored by the time of the claim.
Each read of the subscription sweeps the set and pushes an entry claimed more than `min_idle` ago back onto the queue, where a live consumer takes it.
An ack or a drop removes the entry's record from the set.
`min_idle` has no default, and a recovery set without it does not start.
It has to exceed the longest time one entry takes to handle: a slower handler has its entry returned and handled twice.
The sweep runs once per read, so the `block` interval sets how soon a due entry returns, and one pass moves at most 128 entries.
[`recovery_ttl`](api:ruststream_fred::list::RedisList::recovery_ttl) expires the set once no claim refreshes it, and it has to exceed `min_idle`.
`recovery_zset` turns the [reliable](./reliable.md) form on by itself.

## Example

An invoice claimed by a worker that died is sent by another one a minute later.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_lists.rs#recovery"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_lists.rs#recovery"
```

</TabItem>
</Tabs>
