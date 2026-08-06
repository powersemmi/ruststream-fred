# Redis Streams

A `#[subscriber("key")]` handler binds to a Redis stream key. Because Redis Streams always read
through a consumer group, the bare-string form needs a broker-wide default group
(`.default_group`):

```rust
--8<-- "crates/ruststream-fred/examples/fred_streams.rs:handler"
```

Wire it onto the broker; the `with_broker` / `include` part is identical to every other broker.

```rust
--8<-- "crates/ruststream-fred/examples/fred_streams.rs:app"
```

Payload and headers travel as stream entry fields: the body under a reserved field and each header
under a `h:` prefix, so a round-trip through `XADD` / `XREADGROUP` preserves both.

## Read modes: fresh tail vs reclaim

The read mode is chosen by constructor, never a runtime flag, because the two return disjoint sets
of messages:

- `RedisStream::new(key)` reads fresh entries off the tail (`XREADGROUP >`). This is the normal
  worker.
- `RedisStream::reclaim(key, min_idle)` reclaims entries another consumer fetched but never acked
  (`XAUTOCLAIM`, idle at least `min_idle`). This is crash recovery, run alongside a `new` subscriber
  on the same group ("two handlers per group").

`min_idle` has no default and must exceed the longest legitimate handler runtime: set it too low and
a healthy consumer's in-flight message gets reclaimed and processed twice.

A descriptor can sit directly in the `#[subscriber(...)]` decorator. The fresh-tail worker:

```rust
--8<-- "crates/ruststream-fred/examples/fred_reclaim.rs:worker"
```

The recovery handler on the same group, reclaiming entries idle for over 30 seconds:

```rust
--8<-- "crates/ruststream-fred/examples/fred_reclaim.rs:reclaim"
```

## Repositioning a group

A stream keeps its entries until it is trimmed, so a group can be moved back over history or forward
past a region. `StreamStart` only chooses where a group starts when it is first created; moving a
group that already exists is the `Seekable` capability, which the streams transport implements (the
list transport is destructive and Pub/Sub keeps no history, so neither does).

**A seek is group-wide.** Redis keeps one cursor per consumer group, so moving it repositions every
consumer of that group, not just the subscription that asked - unlike a partitioned log, where a seek
is scoped to one consumer. The type names carry that scope: `RedisGroupPosition` and
`RedisGroupSeeker`.

Three positions, named by constructor:

| Constructor | Where the group resumes |
| --- | --- |
| `RedisGroupPosition::beginning()` | the oldest entry the stream still retains |
| `RedisGroupPosition::end()` | the tail: only entries added afterwards |
| `RedisGroupPosition::after(id)` | the entry following `id` (the cursor is exclusive, like `XGROUP SETID`) |

A `start_at(..)` clause seeks the subscription before its first delivery, on every startup:

```rust
--8<-- "crates/ruststream-fred/examples/fred_seek.rs:start-at"
```

A `Seek` parameter injects the subscription's own seeker, so a handler can move the group while the
service runs:

```rust
--8<-- "crates/ruststream-fred/examples/fred_seek.rs:seek-param"
```

A delivery also reports its own position (`Positioned::position`), and seeking to it delivers that
message again followed by the entries after it - the id is decremented automatically, since the
cursor is exclusive.

What a seek does not touch:

- **the pending entries list.** Entries already delivered and not acknowledged stay pending whichever
  way the cursor moved, and remain reachable through the reclaim path.
- **scheduled delayed retries.** Copies already sitting in a ZSET delay queue are keyed by their due
  time, so they are appended to the stream when they fall due regardless of where the group reads.
- **delivery counts.** A replayed entry is delivered again, so its native delivery count grows; a
  reclaim subscription with `max_deliveries` therefore counts replays towards the poison cap, while
  the framework retry-count header only moves on an actual `nack`.

The cursor changes as soon as the seek returns, but a subscription parked in a blocking `XREADGROUP`
observes it on its next read - within one `block` interval. Entries selected under the old cursor are
discarded rather than delivered.

## Acknowledgement

Settlement follows the republish-retry model:

- `ack` -> `XACK` (remove from the pending list).
- `nack(requeue = true)` -> re-append a copy to the same stream, then `XACK` the original. The copy
  is reprocessed by the normal `new` consumer. This is at-least-once: a crash between the two leaves
  a duplicate.
- `nack(requeue = false)` -> `XACK` to drop.

## Delayed retry

A handler can ask for a delayed redelivery (`HandlerResult::retry_after(delay)`), for example to back
off a transient failure. Redis Streams have no native per-message delay, so by default the runtime
falls back to an in-process timer that re-publishes the message after the delay - at-most-once over
that window, since a crash before the timer fires loses the deferred copy.

For a crash-safe alternative, opt a subscription into a durable ZSET delay queue. It is off by
default and you name the ZSET key explicitly (the key has no sane default):

```rust
--8<-- "crates/ruststream-fred/examples/fred_delayed_retry.rs:handler"
```

A delayed delivery is `ZADD`ed to the named ZSET with a `fire_at` score, then the original is
`XACK`ed; a sweeper folded into the subscription's read loop moves due entries back onto the stream
with `XADD`, so the retry survives a restart. The sweeper's granularity is the read `block` interval,
and the retry-count header is incremented on each pass. An optional TTL on the ZSET key cleans up an
abandoned queue, but it must exceed the longest scheduled delay or pending entries are dropped before
they fire. Scores are wall-clock epoch milliseconds, so keep clocks synced (NTP).
