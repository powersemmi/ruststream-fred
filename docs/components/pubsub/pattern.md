---
sidebar_position: 1
title: RedisPubSubPattern
description: "RedisPubSubPattern reads every channel a glob matches; a retry copy needs a destination named at the mount site."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# RedisPubSubPattern

[`RedisPubSubPattern`](api:ruststream_fred::pubsub::RedisPubSubPattern) reads every channel a glob matches; a retry copy needs a destination named at the mount site.

[`RedisPubSubPattern::new("quotes.*")`](api:ruststream_fred::pubsub::RedisPubSubPattern::new) subscribes with `PSUBSCRIBE`.
It reads `quotes.eu`, `quotes.us` and any other channel the glob matches, with the glob syntax of Redis.
A channel needs no setup in Redis, so a channel first used after startup is read as well.
The subscription's name is the glob itself.
Patterns are classic Pub/Sub: sharded delivery has no pattern subscription, and the descriptor takes no mode.
A handler reads the channel a delivery came on through the [`Channel`](api:ruststream_fred::context::keys::Channel) key of [`PubSubContext`](api:ruststream_fred::context::PubSubContext).
A glob is not a channel a `PUBLISH` can name, so the descriptor offers no address for a [retry copy](../acknowledgement/out-retry.md).
The registration names one with `.out_retry(policy).to(channel)`, and a registration that names none does not start.
The rest of the channel semantics are those of [`RedisPubSub`](./index.md): nothing is kept and nothing is settled.

## Example

The handler reads every `quotes.*` channel.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_pubsub.rs#pattern"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_pubsub.rs#pattern"
```

</TabItem>
</Tabs>

Its registration names `quotes.retry` for retry copies, a channel the glob matches, so a copy comes back to the same handler.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_pubsub.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_pubsub.rs#app"
```

</TabItem>
</Tabs>
