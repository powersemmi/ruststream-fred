---
title: PubSub
description: "RedisPubSub reads one channel; a message reaches only the subscribers connected when it is published, and nothing is settled."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# PubSub

[`RedisPubSub`](api:ruststream_fred::pubsub::RedisPubSub) reads one channel; a message reaches only the subscribers connected when it is published, and nothing is settled.

[`RedisPubSub::new("prices")`](api:ruststream_fred::pubsub::RedisPubSub::new) subscribes to the channel `prices` with `SUBSCRIBE`.
Every subscription of the channel receives every message, in this service and in any other.
Redis keeps nothing: a message published while no subscription is connected is gone.
After a dropped connection the subscription subscribes again, and the messages published while it was down are not delivered.
An outcome settles nothing: an ack, a retry and a drop all report [`AckError::Unsupported`](api:ruststream::AckError::Unsupported), and the runtime logs a warning for each.
A [`retry_after`](../acknowledgement/retry-after.md) still comes back, as a copy the runtime publishes to the channel.
A subscription holds the messages that arrive while its handler is busy in a [buffer](./buffer.md).
[`RedisPubSubPattern`](./pattern.md) reads every channel a glob matches, and [`PubSubMode`](./pubsub-mode.md) chooses between classic and sharded delivery.

## Example

Every price published on `prices` while the service runs.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_pubsub.rs#channel"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_pubsub.rs#channel"
```

</TabItem>
</Tabs>
