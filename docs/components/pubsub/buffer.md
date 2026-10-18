---
sidebar_position: 3
title: buffer
description: "buffer sets how many messages a channel subscription holds while its handler is busy; past it the oldest is lost with a warning."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# buffer

[`buffer`](api:ruststream_fred::pubsub::RedisPubSub::buffer) sets how many messages a channel subscription holds while its handler is busy; past it the oldest is lost with a warning.

Redis pushes every message to the subscription as soon as it is published, whether the handler is ready or not.
The subscription keeps what arrives in a buffer until the handler reads it.
Without the setting the buffer holds `fred`'s default of 32 messages.
When it is full, the oldest message is dropped, and the subscription logs a warning with the channel and the number of messages lost.
A handler that has to absorb a burst sets a buffer as large as the burst.
The room is reserved when the subscription opens, about 600 bytes per message whatever the payloads weigh, because `fred` sizes all eight notification channels of a client by it.
[`RedisPubSubPattern::buffer`](api:ruststream_fred::pubsub::RedisPubSubPattern::buffer) sets the same for a pattern subscription.

## Example

Trades arrive in bursts, so their subscription holds up to 1024 of them.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_pubsub.rs#buffer"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_pubsub.rs#buffer"
```

</TabItem>
</Tabs>
