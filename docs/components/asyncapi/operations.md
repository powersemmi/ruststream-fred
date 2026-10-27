---
sidebar_position: 3
title: Operations
description: "A channel a service publishes to reports where the publish lands, a key or a channel, with its delivery mode, expiry and envelope."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Operations

A channel a service publishes to reports where the publish lands, a key or a channel, with its delivery mode, expiry and envelope.

The publish policy describes the channel, in the word Redis uses for the destination.
A [stream publisher](../publishing/index.md) reports the `key` it appends to and its `envelope`.
A [list publisher](../publishing/list-publish.md) reports its `key`, the `ttlMs` every push re-arms, and its `envelope`.
A [Pub/Sub publisher](../publishing/pubsub-publish.md) reports its `channel`, the `mode` it publishes in, and its `envelope`.

The destination is the one the mount site resolved: a `reply("..")` clause, a reply type's `#[outgoing(name)]`, or an entry of a slot.
So a reply declared with `reply("orders.done")` reports `orders.done`, whichever subscription produced it.
A [dead-letter destination](../acknowledgement/dead-letter.md) is reported as a channel the service publishes to.

## Example

The reply of `orders` goes out on the Pub/Sub channel `orders.done`.
The reply of `jobs` goes to the list `jobs.done`, which expires a minute after the last push.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_asyncapi.rs#operations"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_asyncapi.rs#operations"
```

</TabItem>
</Tabs>
