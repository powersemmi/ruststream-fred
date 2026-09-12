# Redis Streams

A Redis stream is a log: entries stay until the stream is trimmed, and a consumer group gives each
entry to one of its consumers and remembers what was acknowledged.

A streams service imports `ruststream_fred::stream::prelude::*`: the descriptor, the seek types
with the contexts that carry them, and this form's publish policy under the name `Publish`.
`TransactionalPublish` is the same policy under the transactional name, because a stream publisher
buffers on the handle as it is.

A handler file imports `ruststream::prelude::*` instead and names only the capability an injected
publisher needs (`Out<impl Publisher>`, `Out<impl TransactionalPublisher>`), so moving a handler
between forms changes the descriptor and not the handler.

A `#[subscriber("key")]` handler reads the stream under that key. Every read goes through a consumer
group, so the bare-string form needs a broker-wide default group (`.default_group`):

```rust
--8<-- "crates/ruststream-fred/examples/fred_streams.rs:handler"
```

Mount it on the broker:

```rust
--8<-- "crates/ruststream-fred/examples/fred_streams.rs:app"
```

An entry holds the payload in a reserved field and each header under an `h:` prefix, so `XADD` and
`XREADGROUP` preserve both.

## Read modes: fresh tail vs reclaim

A constructor picks the read mode, because the two return disjoint sets of entries:

- `RedisStream::new(key)` reads fresh entries off the tail (`XREADGROUP >`). This is the normal
  worker.
- `RedisStream::reclaim(key, min_idle)` reclaims entries another consumer fetched but never acked
  (`XAUTOCLAIM`, idle at least `min_idle`). This is crash recovery, run alongside a `new` subscriber
  on the same group ("two handlers per group").

`min_idle` has no default. Set it above the longest handler runtime, or a message a healthy consumer
is still processing is reclaimed and handled twice.

A descriptor can be written in the `#[subscriber(..)]` attribute. The fresh-tail worker:

```rust
--8<-- "crates/ruststream-fred/examples/fred_reclaim.rs:worker"
```

The recovery handler on the same group, reclaiming entries idle for over 30 seconds:

```rust
--8<-- "crates/ruststream-fred/examples/fred_reclaim.rs:reclaim"
```

## Batches

A slice parameter makes a handler a batch handler, and its mount site names the batch size:

```rust
--8<-- "crates/ruststream-fred/examples/fred_seek.rs:batch-mount"
```

`batch(n)` is required on a batch handler and rejected on a single-message one. On a stream it
becomes the `COUNT` of the `XREADGROUP` (or `XAUTOCLAIM`) that fetches the batch: the server sends
at most that many entries, and the handler sees exactly what one read returned.

Redis's own read options chain after the size, from `RedisSubscribeExt` in the form preludes.
`block(..)` sets how long one read waits for entries, and a value named there overrides the
descriptor's.

Lists and Pub/Sub pop one entry at a time, so their subscribers assemble batches on the client and
honour the same size.

## Native delivery fields

Redis carries metadata that the payload does not, and a handler reads it by key from its typed
context. A handler names `StreamContext` as its context type, or binds one key with a `Ctx<K>`
parameter; a batch body names `StreamBatchContext`. A key this transport does not carry does not
compile.

| Key | Value | On |
| --- | --- | --- |
| `keys::EntryId` | `EntryId`, the parsed `<milliseconds>-<sequence>` id this delivery was read at | delivery |
| `keys::Position` | `RedisGroupPosition`, the cursor that redelivers this entry | delivery |
| `keys::ConsumerGroup` | the group the subscription reads through | delivery and batch |
| `keys::SeekHandle` | `RedisGroupSeeker`, the group's reposition handle | delivery and batch |

A batch spans many deliveries, so `StreamBatchContext` carries only what belongs to the
subscription. An entry id or a position belongs to one delivery and is read off the batch's own
elements; asking for one on a batch context does not compile.

The reclaim path reports its delivery count and idle time as the `DELIVERY_COUNT_HEADER` and
`IDLE_MS_HEADER` headers, which every transport reads the same way. Pub/Sub has its own
`PubSubContext` (the matched channel, and whether it came through a pattern); a list carries nothing
beyond payload and headers, so its context stays `()`.

## Repositioning a group

A group can be moved back over history or forward past entries it should skip. `StreamStart` chooses
where a group starts when it is created; moving a group that already exists is the `Seekable`
capability, which streams implement.

**A seek is group-wide.** Redis keeps one cursor per consumer group, so a seek repositions every
consumer of that group, not only the subscription that asked. The type names carry that scope:
`RedisGroupPosition` and `RedisGroupSeeker`.

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

While the service runs, a handler seeks through the handle on its own context: `StreamContext`
carries the group's seeker under `keys::SeekHandle`, bound as a `Ctx` parameter.

```rust
--8<-- "crates/ruststream-fred/examples/fred_seek.rs:seek-param"
```

A delivery reports its own position (`Positioned::position`, and the same value under
`keys::Position`). Seeking to that position delivers the message again and then the entries after
it: the cursor is exclusive, so the id is decremented for you.

A batch body seeks the same group. The seeker belongs to the subscription, so it sits on
`StreamBatchContext` under the same key, and the entry the batch reacts to comes from its own
elements:

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

The cursor changes as soon as the seek returns; a subscription blocked in `XREADGROUP` sees it on
its next read, within one `block` interval. Entries the old cursor selected are discarded, not
delivered.

## Acknowledgement

Settlement follows the republish-retry model:

- `ack` -> `XACK` (remove from the pending list).
- `nack(requeue = true)` -> re-append a copy to the same stream, then `XACK` the original. The copy
  is reprocessed by the normal `new` consumer. This is at-least-once: a crash between the two leaves
  a duplicate.
- `nack(requeue = false)` -> `XACK` to drop.

## Delayed retry

`HandlerOutcome::retry_after(delay)` asks for a delayed redelivery, for backing off a transient
error. Redis Streams have no per-message delay, so by default an in-process timer re-publishes the
message when the delay is up, and a crash before it fires loses that copy.

A ZSET delay queue survives a restart. It is off by default, and you name the ZSET key:

```rust
--8<-- "crates/ruststream-fred/examples/fred_delayed_retry.rs:handler"
```

A delayed delivery is `ZADD`ed to that ZSET under its due time with the retry-count header raised by
one, and the original is `XACK`ed. The subscription sweeps the ZSET as it reads and `XADD`s due
entries back onto the stream unchanged. A sweep happens once per read, so the `block` interval is
the granularity, and one pass moves at most 128 due entries. A TTL on the ZSET key cleans up an
abandoned queue and has to be longer than the longest scheduled delay, or entries are dropped before
they fire. Scores are wall-clock epoch milliseconds, so keep clocks synced (NTP).

## Partition keys

`workers(n, by_key)` runs a subscription on several workers and keeps per-key order: deliveries
sharing a partition key go to the same lane. `partition_key` is a step on the publish, so it keys
one message:

<!-- inline-rust: two-publish fragment isolating the step; the compiled call sites are the crate's `partition_key` doctests, which need a connected broker and so cannot double as a snippet source here -->
```rust
use ruststream_fred::stream::prelude::*;
use serde::Serialize;

#[derive(Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

publisher.message(&Order { id: 7 }).partition_key("tenant-a").publish().await?;
publisher.message(&Order { id: 8 }).partition_key("tenant-a").publish().await?;
```

The step sits anywhere after the message, and it takes no position of its own, so it composes with a
declared header contract:

<!-- inline-rust: isolates the contract-plus-key chain; the compiled form is the `partition_key_step_composes_with_a_header_contract` test, whose broker setup would bury the four lines that matter -->
```rust
#[derive(Serialize, Outgoing)]
#[outgoing(name = "orders.keyed", headers = OrderMeta)]
struct KeyedOrder {
    id: u64,
}

#[derive(Serialize)]
struct OrderMeta {
    region: String,
}

publisher
    .message(&KeyedOrder { id: 7 })
    .with_headers(&OrderMeta { region: "eu".into() })
    .partition_key("tenant-a")
    .publish()
    .await?;
```

A handler keys its own publishes the same way. The step is this broker's, so such a body imports
this crate's prelude and names the options type in the slot's bound:

<!-- inline-rust: the signature is the subject here; the compiled form is the `the_step_sets_the_key_the_delivery_reports` test, whose mount and assertions would bury it -->
```rust
use ruststream_fred::stream::prelude::*;

#[derive(OutSlot)]
#[publishes(Order)]
struct Ledger;

#[subscriber(RedisStream::new("orders.in").group("workers"))]
async fn forward(
    order: &Order,
    Out(ledger): Out<impl Publisher<Options = RedisPublishOptions>, Ledger>,
) -> HandlerOutcome {
    if ledger
        .message(order)
        .to("orders.keyed")
        .partition_key("tenant-a")
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}
```

Redis has no partition of its own, so the publisher writes the resolved key into the
`redis-partition-key` header, and that is where the consuming side reads it. Writing that header at
the call site is the spelling that carries across brokers; a step overrides it for its own message,
and a publish with no step leaves the map alone. The header name is public as
`PARTITION_KEY_HEADER`, and all three transports take the step.

## Capabilities

The framework's optional capability traits, and what this broker implements. The notes say where
Lists and Pub/Sub differ from Streams.

| Capability | Native | Notes |
| --- | --- | --- |
| `Subscribe` | yes | Subscribes by stream key through a consumer group (the bare-string form needs `default_group`). Lists and Pub/Sub subscribe through their own descriptors. |
| `BatchSubscriber` | yes, on all three | On Streams natively: the mount site's `batch(n)` is the `COUNT` of the `XREADGROUP` / `XAUTOCLAIM` that fetches the batch, and a batch is one non-empty read, never empty. Lists and Pub/Sub pop one entry at a time, so their subscribers assemble batches on the client and honour the same size. See [Batches](#batches). |
| `TransactionalPublisher` | yes (Streams, standalone and sentinel) | The stream publisher buffers on the handle and commits it as one `MULTI` / `EXEC`. A cluster publisher rejects it, because a `MULTI` block cannot span hash slots. The List and Pub/Sub publishers have no transaction. See [Transactions](transactions.md). |
| `OwnedTransactions` | yes (Streams, standalone and sentinel) | `publisher.transaction()` returns a buffer-owning value, so any number can be open on one handle; cluster is rejected for the same reason. |
| `RequestReply` | no | Redis has no request-reply primitive: nothing on the wire carries a reply address or correlates a reply with its request. |
| `Partitioned` | yes | All three transports read the key from the `redis-partition-key` header for the runtime's `workers(n, by_key)` lanes. The sender sets it with the [`partition_key`](#partition-keys) step. |
| `Seekable` + `Positioned` | yes (Streams) | The group cursor moves with `XGROUP SETID`, and a delivery reports the position that redelivers it. Handlers reach the handle through the `keys::SeekHandle` context key; see [Repositioning a group](#repositioning-a-group). A list is destructive and Pub/Sub keeps no history, so neither implements it. |
| `DescribeServer` | yes | Reports the host and port a client dials (the first seed on cluster and sentinel). A URL's credentials, database number and query stay out of the generated document. A broker built with `RedisBroker::from_pool` reports no host at all: the address sits inside the pool's own configuration. |
