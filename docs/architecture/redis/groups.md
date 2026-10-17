---
title: Groups
sidebar_position: 1
description: "A consumer group reads a stream with one cursor and keeps every entry it delivered and nobody acknowledged in its pending entries list."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "One consumer group over a stream: its cursor, its pending entries list, and the commands that move entries between them",
  "text": "The diagram as text",
  "stream": "Stream orders, as group workers sees it",
  "acked": "acked",
  "pending": "pending",
  "new": "new",
  "cursor": ["Cursor:", "last-delivered-id"],
  "list": ["Pending entries list:", "3-0 to c1, 4-0 to c2"],
  "reader": ["Consumer c1", "reads new entries"],
  "acker": ["Consumer c2", "acknowledges"],
  "claimer": ["Consumer c3", "claims idle ones"],
  "seek": "XGROUP SETID moves the cursor back or forward: a seek",
  "cursorAt": "points at",
  "read": "XREADGROUP",
  "ack": "XACK",
  "claim": "claim"
};

# Groups

A consumer group reads a stream with one cursor and keeps every entry it delivered and nobody
acknowledged in its pending entries list.

<Diagram name="pel" words={words} />

`XREADGROUP` with the id `>` returns the entries past the group's cursor, its
`last-delivered-id`, and moves the cursor. Each entry goes to the one consumer that read it. The
group records the entry in its pending entries list (PEL) with that consumer, the time of the
delivery and a delivery count. `XACK` removes it from the list. The entry stays in the stream:
an acknowledgement is the group's bookkeeping, not a delete.

The crate creates the group when the subscription starts, with `XGROUP CREATE` and `MKSTREAM`, so
a stream that does not exist yet is created empty. A group that already exists is kept as it is.
[`StreamStart`](api:ruststream_fred::stream::StreamStart) decides where a new group's cursor
starts.

An entry whose consumer died stays in the list. Two commands hand it to another consumer once it
has been idle for a given time. `XAUTOCLAIM` scans the list for such entries and transfers them;
[`RedisStream::reclaim`](api:ruststream_fred::stream::RedisStream::reclaim) reads that way, and
asks `XPENDING` for the delivery counts of what it claimed. On Redis 8.4 and later `XREADGROUP`
takes a `CLAIM` option, which returns the idle entries first and then new ones in one call, each
with its idle time and delivery count.
[`RedisStream::claiming`](api:ruststream_fred::stream::RedisStream::claiming) reads that way, and
a claiming subscription refuses to start on an older server.

`XGROUP SETID` moves the cursor. The crate's
[`RedisGroupSeeker`](api:ruststream_fred::RedisGroupSeeker) uses it, so a seek repositions the
whole group, not one consumer. A read that was selected under the old cursor is discarded, and
the next read starts from the new one.

The commands are Redis's own:
[XREADGROUP](https://redis.io/docs/latest/commands/xreadgroup/),
[XACK](https://redis.io/docs/latest/commands/xack/),
[XAUTOCLAIM](https://redis.io/docs/latest/commands/xautoclaim/) and
[XGROUP SETID](https://redis.io/docs/latest/commands/xgroup-setid/).
