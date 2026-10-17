---
title: Subscriptions
description: "A key in #[subscriber] reads that stream through the broker's default group; a descriptor names the form and its settings."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Subscriptions

A key in `#[subscriber]` reads that stream through the broker's default group; a descriptor names the form and its settings.

`#[subscriber("orders")]` reads the Redis stream `orders`.
A stream is read only through a consumer group, and a bare key names none, so the group comes from [`RedisBroker::default_group`](api:ruststream_fred::RedisBroker::default_group).
A bare key on a broker with no default group stops the service at startup, and the error names the setting and the key.
Every other subscription is a descriptor, written where the key goes, as the attribute's first argument.
One descriptor is one form of delivery and carries all of that form's settings:

| Form | Descriptor | What one subscription reads |
| --- | --- | --- |
| Stream | [`RedisStream`](../streams/index.md) | entries of a stream, through a consumer group |
| List | [`RedisList`](api:ruststream_fred::list::RedisList) | elements popped off a list, each by one consumer |
| Channel | [`RedisPubSub`](api:ruststream_fred::pubsub::RedisPubSub) | messages published to a channel while it listens |
| Pattern | [`RedisPubSubPattern`](api:ruststream_fred::pubsub::RedisPubSubPattern) | messages of every channel a glob matches |

The manual form passes the key, or the descriptor itself, to [`subscriber(source, body)`](api:ruststream::runtime::subscriber).
Each subscription reads on a connection of its own, beside the pool that publishes and settles.
The settings of the mount chain apply to every form: the [batch size](./batches.md), the dispatch lanes, the retry cap, the publishers.
[`block`](./block.md) is the one Redis setting among them.

## Example

The stream `orders`, read in the group `workers`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_subscriptions_name.rs#name"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_subscriptions_name.rs#name"
```

</TabItem>
</Tabs>
