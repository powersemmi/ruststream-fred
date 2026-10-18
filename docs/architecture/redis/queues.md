---
title: Queues
sidebar_position: 2
description: "A reliable list moves each entry to a processing list before the handler runs, and removes it from there when the handler settles."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "A reliable list: a claim moves the entry to the processing list, the outcome settles it there, and the recovery set returns orphans",
  "text": "The diagram as text",
  "producer": "Producer",
  "queue": "jobs",
  "processing": "jobs.processing",
  "recovery": ["Recovery set", "sorted by claim time"],
  "handler": "Handler",
  "ack": ["ack:", "LREM"],
  "retry": ["retry: LPUSH", "back, then LREM"],
  "drop": ["drop:", "LREM"],
  "orphan": "Each read first sweeps claims idle past min_idle back to the queue",
  "push": "LPUSH",
  "claim": "BLMOVE",
  "delivered": "delivered",
  "tracked": "ZADD",
  "sweep": "orphans back"
};

# Queues

A reliable list moves each entry to a processing list before the handler runs, and removes it
from there when the handler settles.

<Diagram name="queue" words={words} />

A list is a queue: `LPUSH` adds at one end, a pop takes from the other, and the oldest entry
leaves first. A popped entry is gone from Redis. A simple
[`RedisList`](api:ruststream_fred::list::RedisList) pops with `BRPOP`, so an entry whose handler
fails or whose process dies is lost: delivery is at most once.

A [reliable](api:ruststream_fred::list::RedisList::reliable) list claims instead of popping.
`BLMOVE` moves the entry from the queue to a processing list in one command, and the entry stays
there while the handler runs. The processing list is `<key>.processing` unless the subscription
names another. The outcome settles the entry on the processing list: `LREM` removes it, and a
retry first pushes it back to the queue. Delivery is at least once. Redis describes the pattern
under [LMOVE](https://redis.io/docs/latest/commands/lmove/).

A list keeps no record of who took an entry or when, so a consumer that dies after its claim
leaves the entry on the processing list.
[`recovery_zset`](api:ruststream_fred::list::RedisList::recovery_zset) keeps that record. Every
claim is added to a sorted set, scored by its time. Each read first sweeps the claims older than
`min_idle` and moves each of their entries back to the queue with `LREM` and `LPUSH`, at most 128
per read.

On a cluster both lists have to hash to one slot, because `BLMOVE` touches both keys. A
subscription whose two keys land on different slots refuses to start. A hash tag such as
`{jobs}` puts both on one slot, and the default processing key follows it.
