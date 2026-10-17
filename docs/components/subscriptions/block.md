---
sidebar_position: 1
title: block
description: "block sets how long one read waits on the server: the XREADGROUP BLOCK on a stream, the BRPOP or BLMOVE timeout on a list."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# block

`block` sets how long one read waits on the server: the `XREADGROUP` `BLOCK` on a stream, the `BRPOP` or `BLMOVE` timeout on a list.

The wait is five seconds unless a subscription names another.
A descriptor names it with [`RedisStream::block`](api:ruststream_fred::stream::RedisStream::block) or [`RedisList::block`](api:ruststream_fred::list::RedisList::block).
The mount site names it with the [`block`](api:ruststream_fred::RedisSubscribeExt::block) step, which overrides the descriptor.
The step chains after another step of the mount chain: after `.batch(n)`, after `.pipeline()`, or, in the manual form, after `build()`.
It reads the same before and after `start_at(..)`.
On a [`reclaim`](../streams/reclaim.md) subscription the wait is the pause between two `XAUTOCLAIM` scans that found nothing.
A read wakes as soon as an entry arrives, so the wait costs nothing on a busy stream.
It decides how soon an idle read sees what happens beside it: a [seek](../positions/seek.md) of its group, or a due entry of a delay queue, which the subscription sweeps once per read.
A shorter wait sees them sooner, at the price of one more round trip per idle wait.
The step exists on stream and list subscriptions, the two forms whose read blocks.

## Example

Orders wait at most a second per read; invoices are billed sixteen at a time, with a two-second wait set at the mount site.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_subscriptions_block.rs#block"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_subscriptions_block.rs#block"
```

</TabItem>
</Tabs>
