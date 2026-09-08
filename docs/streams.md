# Redis Streams

A service on this form globs `ruststream_fred::stream::prelude::*`, which carries the descriptor,
the seek types with the contexts that carry them, and this form's publish policy as `Publish` -
plus `TransactionalPublish`, the same policy under the transactional name, since a stream
publisher buffers on the handle and owns transactions as it is.

Two vocabularies, kept apart. A handler file imports `ruststream::prelude::*` and bounds an
injected publisher with the broker capability trait it needs (`Out<impl Publisher>`, `Out<impl
TransactionalPublisher>`); a routes file globs the mode prelude above and names the policy by its
mount-site word, the same word on every form, so moving a handler between forms changes the
descriptor and not the mount.

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

## Batches

A slice payload is what makes a handler a batch handler - nothing in the attribute says it - and its
mount site names one number, the batch size:

```rust
--8<-- "crates/ruststream-fred/examples/fred_seek.rs:batch-mount"
```

`batch(n)` is mandatory on a batch handler and rejected on a single-message one, so the size is
always written where the batch is mounted. On a stream it is the `COUNT` of the `XREADGROUP` (or
`XAUTOCLAIM`) that fetches the batch: the server sends at most that many entries and the batch a
handler sees is the read that produced it - nothing is split or merged on the way.

Everything else about how a read forms stays Redis's own vocabulary and chains after the size,
through `RedisSubscribeExt` (in the form preludes): `block(..)` is how long that read waits for
entries. A descriptor written in the attribute may name it too; the mount site wins.

Lists and Pub/Sub pop one entry at a time, so their subscribers assemble batches on the client and
honour the same size. Nothing at a mount site says which of the two happened, which is the point.

## Native delivery fields

Metadata the transport carries but a payload does not is read by compile-time key off the typed
context, with no hashing, boxing or downcasting. A handler names `StreamContext` as its context type
(or lets a `Ctx<K>` parameter project it), a batch body names `StreamBatchContext`, and a key the
subscription's transport does not carry is a compile error rather than a runtime miss.

| Key | Value | On |
| --- | --- | --- |
| `keys::EntryId` | `EntryId`, the parsed `<milliseconds>-<sequence>` id this delivery was read at | delivery |
| `keys::Position` | `RedisGroupPosition`, the cursor that redelivers this entry | delivery |
| `keys::ConsumerGroup` | the group the subscription reads through | delivery and batch |
| `keys::SeekHandle` | `RedisGroupSeeker`, the group's reposition handle | delivery and batch |

A batch spans many deliveries, so only subscription-scoped fields sit on `StreamBatchContext`: an
entry id or a position belongs to one delivery and rides the batch's own elements instead. The two
are separate types, so a batch body asking for a per-delivery key does not compile.

The reclaim path's native delivery count and idle time are not duplicated here: they arrive as the
`DELIVERY_COUNT_HEADER` / `IDLE_MS_HEADER` headers, where every transport reads them the same way.
Pub/Sub has its own `PubSubContext` (the matched channel, and whether it came through a pattern);
lists carry nothing native beyond payload and headers and stay on the `()` default.

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

While the service runs, the handle comes off the delivery's own context: `StreamContext` carries the
group's seeker under the `keys::SeekHandle` key, so a handler binds it as a `Ctx` parameter and
nothing is attached at the mount site.

```rust
--8<-- "crates/ruststream-fred/examples/fred_seek.rs:seek-param"
```

A delivery also reports its own position (`Positioned::position`, and the same value under the
`keys::Position` key), and seeking to it delivers that message again followed by the entries after
it - the id is decremented automatically, since the cursor is exclusive.

A batch body repositions the same group one level up. The seeker is subscription-scoped, so it rides
the batch context `StreamBatchContext` under that same key, while the entry a batch reacts to is read
off the batch's own elements:

```rust
--8<-- "crates/ruststream-fred/examples/fred_seek.rs:batch"
```

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

A handler can ask for a delayed redelivery (`HandlerOutcome::retry_after(delay)`), for example to back
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

## Partition keys

Running a subscription on several workers (`workers(n, by_key)`) keeps per-key ordering: deliveries
sharing a partition key go to the same lane. Redis has no native partition, so the key travels as a
header. `partition_key` wraps the publisher in an adapter carrying it, so one keyed handle serves
every publish for that key:

<!-- inline-rust: two-publish fragment isolating the keyed handle; the compiled call sites are the crate's `partition_key` doctests, which need a connected broker and so cannot double as a snippet source here -->
```rust
use ruststream_fred::stream::prelude::*;

let tenant = publisher.partition_key("tenant-a");
tenant.message(&Order { id: 7 }).publish().await?;
tenant.message(&Order { id: 8 }).publish().await?;
```

The key rides underneath the publish's own headers, so it composes with a declared header contract:

<!-- inline-rust: isolates the contract-plus-key chain; the compiled form is the `partition_key_step_composes_with_a_header_contract` test, whose broker setup would bury the four lines that matter -->
```rust
publisher
    .partition_key("tenant-a")
    .message(&Order { id: 7 })
    .with_headers(&OrderMeta { region: "eu".into() })
    .publish()
    .await?;
```

Naming `redis-partition-key` in a publish's own headers overrides the handle's key for that message;
naming any other header leaves it in place. The adapter is available on all three transports'
publishers, and the header name is public as `PARTITION_KEY_HEADER`.

## Capabilities

Which of the framework's optional capability traits this broker implements natively. Streams
implements the most of the three transports; the notes name where Lists and Pub/Sub differ.

| Capability | Native | Notes |
| --- | --- | --- |
| `Subscribe` | yes | Subscribes by stream key through a consumer group (the bare-string form needs `default_group`). Lists and Pub/Sub subscribe through their own descriptors. |
| `BatchSubscriber` | yes, on all three | On Streams natively: the mount site's `batch(n)` is the `COUNT` of the `XREADGROUP` / `XAUTOCLAIM` that fetches the batch, and a batch is one non-empty read, never empty. Lists and Pub/Sub pop one entry at a time, so their subscribers assemble batches on the client and honour the same size. See [Batches](#batches). |
| `TransactionalPublisher` | yes (Streams, standalone and sentinel) | The stream publisher buffers on the handle and commits it as one `MULTI` / `EXEC`. A cluster publisher rejects it, because a `MULTI` block cannot span hash slots. The List and Pub/Sub publishers have no transaction. See [Transactions](transactions.md). |
| `OwnedTransactions` | yes (Streams, standalone and sentinel) | `publisher.transaction()` returns a buffer-owning value, so any number can be open on one handle; cluster is rejected for the same reason. |
| `RequestReply` | no | Redis has no request-reply primitive: nothing on the wire carries a reply address or correlates a reply with its request. |
| `Partitioned` | yes | All three transports read the key from the `redis-partition-key` header for the runtime's `workers(n, by_key)` lanes. The sender sets it, with [`partition_key`](#partition-keys). |
| `Seekable` + `Positioned` | yes (Streams) | The group cursor moves with `XGROUP SETID`, and a delivery reports the position that redelivers it. Handlers reach the handle through the `keys::SeekHandle` context key; see [Repositioning a group](#repositioning-a-group). A list is destructive and Pub/Sub keeps no history, so neither implements it. |
| `DescribeServer` | yes | Reports the configured address (the first seed on cluster and sentinel). |
