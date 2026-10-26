---
sidebar_position: 3
title: Framing
description: "A list entry or a channel message carries its headers in a binary frame, or in a readable envelope when both ends set one codec."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Framing

A list entry or a channel message carries its headers in a binary frame, or in a readable envelope when both ends set one codec.

A stream entry has fields, so it keeps the payload in the field `_payload` and each header in a field named `h:<header>`.
A list element and a channel message are one value, so the headers travel in a frame around the payload.
The default frame is a compact binary one, which tools show as bytes; it opens with a marker header, and a message without headers is written as its bare payload.
A codec set on both ends frames the value as a `{headers, payload}` envelope serialized with that codec.
With JSON, `redis-cli` and RedisInsight show the envelope as text.
The reading end sets it with [`RedisList::codec`](api:ruststream_fred::list::RedisList::codec), [`RedisPubSub::codec`](api:ruststream_fred::pubsub::RedisPubSub::codec) or [`RedisPubSubPattern::codec`](api:ruststream_fred::pubsub::RedisPubSubPattern::codec).
The writing end sets it with [`RedisListPublish::codec`](api:ruststream_fred::list::RedisListPublish::codec) or [`RedisPubSubPublish::codec`](api:ruststream_fred::pubsub::RedisPubSubPublish::codec).
The broker's default publisher writes a list or a channel this service reads in that subscription's framing.
Both framings carry any bytes.
The envelope writes a field whose bytes are valid UTF-8 as text and any other bytes as themselves.
A subscription with a codec reads either framing.
A subscription without one reads an envelope as its payload, and the decode of that payload fails.
A value an outside client wrote without a frame arrives as the payload with no headers, whatever its first bytes are.
A payload of this service's that carries the marker where a frame carries it is framed, so it reads back unchanged.
A binary frame written by 0.7.1 or an earlier release has no marker and reads as its payload, frame bytes included, so services that share a list or a channel upgrade together.
The codecs `cbor` and `msgpack` of the core frame the envelope the same way.

## Example

Orders become jobs on a list, with the tenant in a header; both ends name JSON.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_subscriptions_framing.rs#framing"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_subscriptions_framing.rs#framing"
```

</TabItem>
</Tabs>
