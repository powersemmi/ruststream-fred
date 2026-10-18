---
title: Settlement
sidebar_position: 5
description: "What ack, retry, drop and retry_after become on a Redis stream, a reliable list, a simple list and Pub/Sub."
---

import {Diagram} from '@site/src/components/Diagrams';

export const table = {
  "title": "The commands each outcome sends, per form: a stream, a claiming stream, a reliable list, and the forms with no acknowledgement",
  "text": "The diagram as text",
  "outcome": "Outcome",
  "stream": "Stream",
  "claiming": "Claiming",
  "reliable": "Reliable",
  "none": "No ack",
  "ack": "ack",
  "retry": "retry",
  "drop": "drop",
  "ack-stream": "XACK",
  "ack-claiming": "XACK",
  "ack-reliable": "LREM",
  "ack-none": ["not", "supported"],
  "retry-stream": ["XADD copy,", "then XACK"],
  "retry-claiming": ["stays", "pending"],
  "retry-reliable": ["LPUSH,", "then LREM"],
  "retry-none": ["not", "supported"],
  "drop-stream": "XACK",
  "drop-claiming": "XACK",
  "drop-reliable": "LREM",
  "drop-none": ["not", "supported"]
};

export const delays = {
  "title": "The three ways a stream subscription serves retry_after: a copy in the process, a copy in a Redis sorted set, or the entry left pending",
  "text": "The diagram as text",
  "copyLane": "Runtime copy",
  "zsetLane": "Delay queue",
  "claimingLane": "Claiming",
  "outcome": "retry_after(delay) on a stream entry",
  "copyAck": ["XACK the", "original"],
  "held": ["Copy held in", "the process:", "lost on a crash"],
  "republish": ["XADD the copy", "when the timer", "fires"],
  "zadd": ["ZADD the copy,", "then XACK"],
  "zset": ["In Redis,", "scored by its", "due time"],
  "sweep": ["A read sweeps", "it: XADD back", "to the stream"],
  "pending": ["Stays in", "the pending", "entries list"],
  "claimed": ["Claimed back", "after", "min_idle"],
  "timer": "delay",
  "due": "due",
  "idle": "idle"
};

# Settlement

What `ack`, `retry`, `drop` and `retry_after` become on a Redis stream, a reliable list, a simple
list and Pub/Sub.

<Diagram name="settlement" words={table} />

The runtime settles each delivery with the [outcome](../components/acknowledgement/index.md) its
handler returned, and the crate maps the outcome onto the form's own acknowledgement. A stream
acknowledges with `XACK` in the subscription's group. A reliable list removes the entry from its processing list with `LREM`. A
simple list and Pub/Sub have nothing to acknowledge, and settling there answers
`AckError::Unsupported`. The settle commands go out on the pool, not on the subscription's read
client.

A retry on a stream appends a copy of the entry to the same stream, then acknowledges the
original. A crash between the two leaves a duplicate, not a loss. A
[claiming](api:ruststream_fred::stream::RedisStream::claiming) subscription retries through the
pending entries list instead. It appends and acknowledges nothing, and its next read claims the
entry back once it has been idle for `min_idle`, under the same id, with a delivery count one
higher. A reliable list pushes the entry back to its queue before it removes it from the
processing list.

## Retries with a delay

<Diagram name="delay" words={delays} />

Redis has no per-entry delay, so a stream subscription serves
[`retry_after`](api:ruststream::runtime::HandlerOutcome::retry_after) in one of three ways. By
default the runtime acknowledges the original and holds a copy, which it publishes back to the
stream when the delay is up. The copy's retry count is one higher. A crash before the timer fires
loses it.

[`delayed_retry`](api:ruststream_fred::stream::RedisStream::delayed_retry) with
[`DelayedRetry::DurableZset`](api:ruststream_fred::DelayedRetry::DurableZset)
[keeps the copy in Redis](../components/acknowledgement/delayed-retry.md). The copy is added to a
sorted set, scored by its due time, and then the original is acknowledged. Each read first moves
the copies that are due back onto the stream with `XADD`, at most 128 per read, so a delay is honoured to within one `block` interval. The scores are
wall-clock milliseconds, so the clocks of the services that share a queue should agree.

A claiming subscription leaves the entry pending, and its next read claims it back once
`min_idle` has passed. A delay shorter than `min_idle` waits `min_idle`.

A list and a Pub/Sub subscription take the runtime's copy, which goes back to the list or the
channel. A pattern subscription names the destination of its copies at its mount site.

A `max_attempts` cap counts attempts with one of two numbers, and the form decides which. The
claiming and reclaim modes count with the server's delivery count from the pending entries list.
Every other form counts with the retry-count header that the copies carry, which the delay queue
raises on every round.
