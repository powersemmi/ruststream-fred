---
title: RedisPublish
description: "RedisPublish, Publish in the stream prelude, appends each message to a stream with XADD."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# RedisPublish

[`RedisPublish`](api:ruststream_fred::RedisPublish), `Publish` in the stream prelude, appends each message to a stream with `XADD`.

A mount site names it wherever a registration publishes: `.out_reply(Publish)` for a reply, `.out(Slot, Publish)` for a slot, `.out_retry(Publish)` for [retry copies](../acknowledgement/out-retry.md).
The policy holds no connection and no setting.
The runtime pairs it with the connected broker at startup into a [`RedisPublisher`](api:ruststream_fred::RedisPublisher), so a handler never holds a publisher that is not connected.
The message's name is the stream key, and the entry id is `*`, the one Redis assigns.
The payload becomes the entry's `_payload` field, and each header a field of its own named `h:<header>`.
A publish returns once Redis has answered the `XADD`.

The stream prelude also names the policy `TransactionalPublish`: the stream publisher carries both [transaction](../transactions/index.md) kinds.
A list or a channel has its own policy: [`RedisListPublish`](./list-publish.md) and [`RedisPubSubPublish`](./pubsub-publish.md).
A registration that names no policy for its reply publishes through [`RedisDefaultPublish`](./default.md).
Per message, a publish carries a [partition key](./partition-key.md).
Outside an app, [`ConnectedRedisBroker::publisher`](api:ruststream_fred::ConnectedRedisBroker::publisher) hands out the same publisher.

A reply goes to the stream its type declares with `#[outgoing(name = "..")]`, or to the one the mount site names in `reply("..")`.
Redis carries no reply address and no correlation of its own: a reply is an ordinary publish to a name both sides agree on.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_publishing.rs#publish"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_publishing.rs#publish"
```

</TabItem>
</Tabs>
