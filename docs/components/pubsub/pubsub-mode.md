---
sidebar_position: 2
title: PubSubMode
description: "PubSubMode picks classic SUBSCRIBE and PUBLISH, broadcast to every node, or sharded SSUBSCRIBE and SPUBLISH, local to a slot."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# PubSubMode

[`PubSubMode`](api:ruststream_fred::pubsub::PubSubMode) picks classic `SUBSCRIBE` and `PUBLISH`, broadcast to every node, or sharded `SSUBSCRIBE` and `SPUBLISH`, local to a slot.

[`PubSubMode::Classic`](api:ruststream_fred::pubsub::PubSubMode::Classic) is the default.
On a cluster a classic message travels to every node, so every node carries the traffic of every channel.
It is the mode for standalone and sentinel servers, and the only one a [pattern](./pattern.md) has.
[`PubSubMode::Sharded`](api:ruststream_fred::pubsub::PubSubMode::Sharded) needs Redis 7 or later.
A sharded channel lives on the node that owns its hash slot, so its traffic stays there and a cluster scales with the number of channels.
The two modes do not reach each other: a sharded subscriber hears only `SPUBLISH`, and a classic one only `PUBLISH`.
So the subscription sets its mode with [`RedisPubSub::mode`](api:ruststream_fred::pubsub::RedisPubSub::mode), and a publish policy aimed at it sets the same one with [`RedisPubSubPublish::mode`](api:ruststream_fred::pubsub::RedisPubSubPublish::mode).
One service reads a channel in one mode: a second subscription that reads it in the other mode does not start.

## Example

Orders arrive on a sharded channel, and the shipment answering each one leaves with `SPUBLISH` to a sharded channel a tracker reads.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_pubsub_sharded.rs#sharded"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_pubsub_sharded.rs#sharded"
```

</TabItem>
</Tabs>
