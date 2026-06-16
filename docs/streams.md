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
