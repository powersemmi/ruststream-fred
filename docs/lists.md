# Redis Lists (work queue)

A Redis list is a work queue with competing consumers: a producer `LPUSH`es an entry, consumers pop
from the right, and exactly one of them gets it.

A list service imports `ruststream_fred::list::prelude::*`: the descriptor and this form's publish
policy under the name `Publish`.

Simple mode pops with `BRPOP` and settles nothing: a crash loses the entry (at-most-once):

```rust
--8<-- "crates/ruststream-fred/examples/fred_list.rs:simple"
```

Reliable mode moves the entry to a processing list and removes it when the handler acks, so a crash
means the job is repeated, not lost (at-least-once):

```rust
--8<-- "crates/ruststream-fred/examples/fred_list.rs:reliable"
```

The `RedisListPublish` policy publishes with `LPUSH`. You name it where the handler is mounted, and
the runtime constructs the publisher from it on the connected broker; outside an app,
`connected.list_publisher(RedisListPublish::new())` returns one directly.

A list entry is framed like a [Pub/Sub](pubsub.md) message: a binary frame that carries any bytes, or
a codec-serialized envelope when the same codec is set on both ends (`.codec(JsonCodec)`).

A pop returns one entry, so the subscriber assembles batches itself. A batch handler names its size
with `batch(n)` where it is mounted and never sees a longer batch; `block(..)` chains after the size,
as it does on a stream (see [Batches](streams.md#batches)).

## Orphan recovery

A consumer that dies mid-handler leaves its entry on the processing list, because Redis lists track
nothing as pending. Naming a ZSET key turns on a recovery watchdog, off by default:

```rust
--8<-- "crates/ruststream-fred/examples/fred_list.rs:recovery"
```

The subscription records each claim in the ZSET under its claim time, and sweeps the ZSET as it
reads. An entry idle longer than `min_idle` goes back to the main list, where a live consumer takes
it again. Set `min_idle` above the longest handler runtime: a shorter one recovers an entry that is
still being processed, and the job runs twice. `recovery_ttl` expires an abandoned ZSET key and has
to be longer than `min_idle`. For a durable queue that recovers on its own, use Redis Streams.

## List publisher TTL

A TTL on the list key bounds an idle queue:
`RedisListPublish::new().ttl(Duration::from_secs(300))` re-arms a `PEXPIRE` at every publish, so a
queue in use never expires and an idle one lapses. The TTL is off by default and covers the whole
list: Redis lists have no per-entry expiry.
