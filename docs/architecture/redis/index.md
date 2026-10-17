---
title: Transports
sidebar_position: 0
description: "Redis carries messages three ways: a stream keeps entries for consumer groups, a list hands each entry to one consumer, a channel keeps nothing."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "A producer writes to a stream, a list and a channel, and each transport hands the message to its readers differently",
  "text": "The diagram as text",
  "streamProducer": "XADD",
  "listProducer": "LPUSH",
  "channelProducer": "PUBLISH",
  "stream": ["Stream", "keeps every entry"],
  "list": ["List", "until popped"],
  "channel": ["Channel", "keeps nothing"],
  "groupA": ["Group", "billing"],
  "groupB": ["Group", "audit"],
  "workers": ["Consumers", "take turns"],
  "subA": ["Client", "A"],
  "subB": ["Client", "B"],
  "late": ["Joins later:", "never sees it"],
  "keeps": ["Each group reads", "every entry"],
  "once": ["Each entry goes", "to one consumer"]
};

# Transports

Redis carries messages three ways: a stream keeps entries for consumer groups, a list hands each
entry to one consumer, a channel keeps nothing.

<Diagram name="transports" words={words} />

A stream is an append-only log. `XADD` appends an entry under an id the server assigns, and the
entry stays until the stream is trimmed. A consumer group reads the stream with a cursor of its
own, so two groups each receive every entry. Inside one group, each entry goes to one consumer.
[Groups](groups.md) draws how a group tracks what it delivered.

A list is a queue. A producer pushes with `LPUSH`, a consumer pops from the other end, and a
popped entry is gone from Redis. [Queues](queues.md) draws how a reliable list keeps an entry until
its handler settles it.

A channel stores nothing. `PUBLISH` pushes a message to the clients subscribed at that moment, and
a client that subscribes later never sees it. [Broadcast](broadcast.md) draws it.

A service reads each transport through its own descriptor:
[`RedisStream`](api:ruststream_fred::stream::RedisStream),
[`RedisList`](api:ruststream_fred::list::RedisList) and
[`RedisPubSub`](api:ruststream_fred::pubsub::RedisPubSub). The handler is the same
`#[subscriber]` function on all three. What an acknowledgement means differs, as
[Settlement](../settlement.md) draws.
