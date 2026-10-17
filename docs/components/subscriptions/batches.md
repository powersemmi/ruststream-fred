---
sidebar_position: 2
title: Batches
description: "A slice handler reads one XREADGROUP COUNT on a stream; a list or a channel builds its batch on the client up to the same size."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Batches

A slice handler reads one `XREADGROUP` `COUNT` on a stream; a list or a channel builds its batch on the client up to the same size.

A handler over `&[T]` is a batch handler, and [`batch(n)`](api:ruststream::runtime::SubscriberSettings::batch) at the mount site is its largest batch.
On a stream the size is the `COUNT` of the read that fetches the batch, `XREADGROUP` or `XAUTOCLAIM`.
The server sends at most `n` entries, and the handler gets exactly what one read returned, never an empty batch.
A list and a channel deliver one value at a time.
Their subscriptions gather a batch on the client from the values already waiting, up to the same `n`, and hand it over without waiting for more.
So a batch is often smaller than `n`, and a quiet key yields batches of one.
One outcome settles every entry of the batch.
In the manual form the body is an `impl Handle<[T]>`, where `Ok(())` settles the batch with an ack and `Err(outcomes)` settles entry `i` with outcome `i`.
A stream batch reads a [`StreamBatchContext`](api:ruststream_fred::context::StreamBatchContext), which carries the group and its [seek handle](../positions/seek.md) and no field of one entry.
The [`block`](./block.md) step chains after the size.

## Example

Three values waiting on each form arrive in one call.
The stream handler and the app:

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_subscriptions_batches.rs#stream"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_subscriptions_batches.rs#stream"
```

</TabItem>
</Tabs>

The list and the channel handlers:

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_subscriptions_batches.rs#client"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_subscriptions_batches.rs#client"
```

</TabItem>
</Tabs>
