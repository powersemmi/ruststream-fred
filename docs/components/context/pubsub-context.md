---
sidebar_position: 4
title: PubSubContext
description: "PubSubContext gives a channel handler the channel a message arrived on and whether a pattern matched it."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# PubSubContext

[`PubSubContext`](api:ruststream_fred::context::PubSubContext) gives a channel handler the channel a message arrived on and whether a pattern matched it.

The subscription builds it once per message, on a [`RedisPubSub`](api:ruststream_fred::pubsub::RedisPubSub) channel and a [`RedisPubSubPattern`](api:ruststream_fred::pubsub::RedisPubSubPattern) glob alike.
On a channel subscription the channel is the one it names.
On a pattern subscription it is the concrete channel the glob matched, so it differs from message to message.

| Key | Value | What it is |
| --- | --- | --- |
| [`Channel`](api:ruststream_fred::context::keys::Channel) | `&str` | the channel the message was published to |
| [`FromPattern`](api:ruststream_fred::context::keys::FromPattern) | `bool` | `true` when a `PSUBSCRIBE` glob matched the message |
| [`FredPool`](api:ruststream_fred::context::keys::FredPool) | `fred::clients::Pool` | the broker's connection pool, as [FredPool](./fred-pool.md) shows |

A handler names the context in its signature, `ctx: &mut Context<'_, PubSubContext>`, and reads each field with `ctx.context(key)`.
A pattern subscription names where its retry copies go with `out_retry`, because a glob is no channel a copy could be published to.

## Example

One handler hears every `events.*` channel and records the channel each event came from.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_context_pubsub.rs#pubsub"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_context_pubsub.rs#pubsub"
```

</TabItem>
</Tabs>
