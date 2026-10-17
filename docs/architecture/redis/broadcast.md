---
title: Broadcast
sidebar_position: 3
description: "Redis Pub/Sub pushes a message to the subscriptions connected when it is published, and keeps nothing for anyone else."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "Pub/Sub: a publish reaches the subscriptions connected at that moment, each through its client's buffer",
  "text": "The diagram as text",
  "publisher": ["Publisher", "PUBLISH news"],
  "channel": ["Channel news", "keeps nothing"],
  "subA": ["Subscription A", "connected"],
  "subB": ["Subscription B", "connected"],
  "late": ["Subscribes", "later:", "never sees it"],
  "buffer": ["Client buffer", "32 by default"],
  "handler": "Handler",
  "overflow": ["A handler that falls behind its buffer", "loses the oldest messages"],
  "publish": "publish"
};

# Broadcast

Redis Pub/Sub pushes a message to the subscriptions connected when it is published, and keeps
nothing for anyone else.

<Diagram name="broadcast" words={words} />

A channel is a name, not a key: Redis stores nothing under it. `PUBLISH` pushes the message to
every client subscribed to the channel at that moment. Redis calls this at-most-once delivery: a
client that is not subscribed, or loses its connection, never gets the message
([Pub/Sub](https://redis.io/docs/latest/develop/pubsub/)).

Each [`RedisPubSub`](api:ruststream_fred::pubsub::RedisPubSub) subscription opens a client of its
own and subscribes it. A classic subscription returns once a `PING` on the same connection has
been answered, so a message published after the subscription opened reaches it.
[`RedisPubSubPattern`](api:ruststream_fred::pubsub::RedisPubSubPattern) subscribes to a glob with
`PSUBSCRIBE` and receives every channel that matches it.

A message waits in the client's buffer until the handler takes it. The buffer holds 32 messages,
`fred`'s default, unless [`buffer`](api:ruststream_fred::pubsub::RedisPubSub::buffer) sets
another size. A subscription that falls behind loses the oldest messages and logs a warning with
the channel and the count.

Classic Pub/Sub sends every message to every node of a cluster. Sharded Pub/Sub, `SSUBSCRIBE` and
`SPUBLISH` since Redis 7.0, assigns a channel to a hash slot and keeps its messages inside that
shard. [`PubSubMode`](api:ruststream_fred::pubsub::PubSubMode) picks one, and a subscription in
one mode does not receive what is published in the other.

A Pub/Sub delivery has nothing to settle: `ack` and `nack` answer `AckError::Unsupported`.
