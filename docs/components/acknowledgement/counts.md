---
sidebar_position: 4
title: Counts
description: "The claiming modes report Redis's delivery count and idle time as headers, and a cap reads that count."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Counts

The claiming modes report Redis's delivery count and idle time as headers, and a cap reads that count.

A consumer group counts how often it has handed out each pending entry.
The two stream modes that claim entries, [`claiming`](api:ruststream_fred::stream::RedisStream::claiming) and [`reclaim`](api:ruststream_fred::stream::RedisStream::reclaim), put that count on every delivery in [`DELIVERY_COUNT_HEADER`](api:ruststream_fred::DELIVERY_COUNT_HEADER).
[`IDLE_MS_HEADER`](api:ruststream_fred::IDLE_MS_HEADER) carries how long the entry had been pending, in milliseconds.
A `claiming` delivery reports the attempts made before it, so a fresh entry carries zero for both headers.
A `reclaim` delivery reports the count including itself.
The runtime reads the same count through [`redelivery_count`](api:ruststream::IncomingMessage::redelivery_count), which counts the current delivery in both modes.
That is the count a [`max_attempts`](./dead-letter.md) cap reads.
An entry a crashed worker took and never settled counts towards the cap, although no process saw it fail.
A reclaiming read that found entries issues one `XPENDING` to learn their counts.
Every other subscription has no count from Redis, and its cap counts through the runtime's retry-count header instead.

## Example

The handler logs both values with every attempt.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_acknowledgement_counts.rs#counts"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_acknowledgement_counts.rs#counts"
```

</TabItem>
</Tabs>

The cap at the mount site reads the same count: the third failed delivery goes to `payouts.dlq`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_acknowledgement_counts.rs#declaration"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_acknowledgement_counts.rs#declaration"
```

</TabItem>
</Tabs>
