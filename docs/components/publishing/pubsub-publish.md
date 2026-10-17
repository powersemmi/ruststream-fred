---
sidebar_position: 2
title: RedisPubSubPublish
description: "RedisPubSubPublish, Publish in the Pub/Sub prelude, sends with PUBLISH or SPUBLISH in the framing its codec sets."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# RedisPubSubPublish

[`RedisPubSubPublish`](api:ruststream_fred::pubsub::RedisPubSubPublish), `Publish` in the Pub/Sub prelude, sends with `PUBLISH` or `SPUBLISH` in the framing its codec sets.

The message's name is the channel.
[`mode`](api:ruststream_fred::pubsub::RedisPubSubPublish::mode) picks the command: [`PubSubMode::Classic`](api:ruststream_fred::pubsub::PubSubMode), the default, sends `PUBLISH`, and `PubSubMode::Sharded` sends `SPUBLISH` to the channel's slot.
The mode matches the subscribers': a sharded message reaches only `SSUBSCRIBE` subscriptions.
Headers travel beside the payload in a binary frame by default.
[`codec`](api:ruststream_fred::pubsub::RedisPubSubPublish::codec) frames them with a codec instead, which makes the value readable when the data is text; the subscribers set the same codec.
A payload that is not text is published byte for byte either way.

The policy pairs into a [`RedisPubSubPublisher`](api:ruststream_fred::pubsub::RedisPubSubPublisher) at startup.
A message reaches the subscriptions connected when it is sent; Redis keeps no copy for a subscriber that connects later.
A publish to a channel nobody hears succeeds.

## Example

Each event is answered with an audit entry, sent with `PUBLISH` to the `audit` channel its type declares.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_publishing_pubsub.rs#pubsub_publish"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_publishing_pubsub.rs#pubsub_publish"
```

</TabItem>
</Tabs>
