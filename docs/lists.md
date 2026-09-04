# Redis Lists (work queue)

A service on this form globs `ruststream_fred::list::prelude::*`, which carries the descriptor and
this form's publish policy as `Publish`.

A list is a competing-consumers queue: a producer `LPUSH`es, consumers pop from the right, and each
entry goes to exactly one consumer (no fan-out, no replay). Simple mode is at-most-once (`BRPOP`, no
ack):

```rust
--8<-- "crates/ruststream-fred/examples/fred_list.rs:simple"
```

Reliable mode moves each entry to a processing list and removes it on ack (at-least-once), so a
crashed handler does not silently lose its job:

```rust
--8<-- "crates/ruststream-fred/examples/fred_list.rs:reliable"
```

Publish with the `RedisListPublish` policy (`LPUSH`): attach it where the handler is mounted and the
runtime pairs it with the connected broker, or call
`connected.list_publisher(RedisListPublish::new())` outside an app. Headers travel in the same frame
as Pub/Sub: a lossless binary frame by default, or a readable codec-serialized envelope when a codec
is set on both ends (`.codec(JsonCodec)`).

A pop returns one entry, so a page handler on a list is served by pages the subscriber assembles on
the client; it still names its size with `batch(n)` at the mount site and never sees a longer page.
`block(..)` chains after the size, as it does on a stream (see [Pages](streams.md#pages)).

## Orphan recovery

A dead consumer can strand a reliable entry on its processing list, since Redis lists have no native
pending tracking. Opt into a recovery watchdog by naming a ZSET key (off by default):

```rust
--8<-- "crates/ruststream-fred/examples/fred_list.rs:recovery"
```

Each claim is recorded in the ZSET (score = claim time); a sweeper folded into the subscription's
read loop returns entries idle past `min_idle` to the main list, where a live consumer re-claims
them. Like the Streams reclaim path, `min_idle` must exceed the longest legitimate handler runtime,
or a still-running entry is recovered and processed twice. An optional `recovery_ttl` cleans up an
abandoned ZSET key but must exceed `min_idle`. Without recovery, Redis Streams remain the recommended
durable, recoverable path.

## List publisher TTL

An idle list can be bounded with a key TTL: `RedisListPublish::new().ttl(Duration::from_secs(300))`
re-arms a `PEXPIRE` on the list key on every publish, so an actively used queue never expires and
only an idle one lapses. It is off by default and per-key (the whole list), not per-entry - Redis
lists have no per-element expiry. Pub/Sub has no equivalent (`PUBLISH` stores nothing to expire), and
streams bound their size with trimming rather than a TTL.
