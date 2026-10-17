---
title: Window
sidebar_position: 6
description: "A pipelined subscription settles in a window: the deliveries of a read queue their side effects, and one flush sends them with the settles."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "One read of a pipelined stream subscription: each delivery queues a command, the outcome keeps or drops it, and one flush sends the rest",
  "text": "The diagram as text",
  "entry": "Entry {id}",
  "queues": "queues an INCR",
  "kept": ["ack:", "segment kept"],
  "dropped": ["drop:", "segment dropped"],
  "flush": "One flush, one round trip",
  "segment": "INCR of {id}",
  "settleOnly": "settle only",
  "atomic": ["Under .atomic() each segment is MULTI, its commands,", "the delivery's own settle, then EXEC"]
};

# Window

A pipelined subscription settles in a window: the deliveries of a read queue their side effects,
and one flush sends them with the settles.

<Diagram name="window" words={words} />

[`.pipeline()`](api:ruststream_fred::pipeline::RedisPipelineSteps::pipeline) at the mount site
opens a window over the subscription. Each delivery of a read takes a slot in it. What its handler
queues through `Ctx<keys::Pipeline>` goes into the slot's segment, a `fred` pipeline opened at the
first queued command. The handler does not see the command's reply.

The outcome decides the segment: `ack` keeps it, and every other outcome drops it. The settle goes
out either way, the one the form sends without a window.

A window flushes when the read's `COUNT` has settled, when nothing is outstanding, or when the
subscription stops. The flush sends the kept segments in the order they were committed, then the
settles of the read in one pipeline, on a connection of the pool. A stream's settles leave as one
`XACK`, and a reliable list's as one `LREM` per entry. Under load that is one round trip per read.

[`.atomic()`](api:ruststream_fred::pipeline::RedisPipelineSteps::atomic) wraps each segment in
`MULTI` and `EXEC` and puts the delivery's own settle inside it. The handler's side effects and
its acknowledgement then take effect together. Redis has no rollback: a command that fails inside
`EXEC` leaves the others executed
([transactions](https://redis.io/docs/latest/develop/using-commands/transactions/)). On a cluster
every key of a segment has to share the slot of the subscription's key.

A flush that fails is reported once on the delivery stream. On the forms that settle, an entry
whose settle failed stays pending and is delivered again.
