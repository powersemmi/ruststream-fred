Redis and Valkey broker for the [`RustStream`](https://github.com/powersemmi/ruststream)
messaging framework, backed by the [`fred`] client.

Three Redis transports sit behind one broker. Redis Streams is the durable one, a log read
through consumer groups with acknowledgement, redelivery and crash recovery, and the form to
reach for unless a service has a reason not to. A Redis list is a competing-consumers work
queue. Pub/Sub is fan-out with no acknowledgement at all. Four topologies serve all three: a
standalone server, a cluster, a sentinel set, and a `fred` `Pool` a service built for itself.

The lifecycle is the framework's ladder of consuming transitions. [`RedisBroker`] records the
topology synchronously and does no I/O, so a service fits the synchronous `#[ruststream::app]`
builder; [`Broker::connect`](ruststream::Broker::connect) yields the [`ConnectedRedisBroker`]
every subscription and publisher is reached from, and
[`ConnectedBroker::shutdown`](ruststream::ConnectedBroker::shutdown) yields the terminal
[`ClosedRedisBroker`]. A publisher that outlives the connection reports
[`RedisError::ShutDown`] instead of succeeding against a dead pool.

Installation, the transport templates and the list of brokers are on the site:
<https://powersemmi.github.io/ruststream-fred/>. The framework's own surface (routers, the
per-delivery context, typed headers, middleware, failure policies) is documented at
<https://docs.rs/ruststream/latest/ruststream/runtime/index.html>.

[`fred`]: https://docs.rs/fred

# A service

A handler is an `async fn` over a decoded payload. Every Redis stream is read through a consumer
group, so the bare-string subscriber form needs a broker-wide default group; naming the
[`RedisStream`] descriptor instead is what a subscription with settings of its own does.

```
# mod demo {
use ruststream_fred::stream::prelude::*;
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u64,
}

#[subscriber("orders")]
async fn handle(order: &Order) -> HandlerOutcome {
    println!("got order {}", order.id);
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379").default_group("workers"),
        |b| {
            b.include(handle);
        },
    )
}
# }
# fn main() {}
```

`cargo run -- run` starts it. An entry holds the payload in a reserved field and each header
under an `h:` prefix, so `XADD` and `XREADGROUP` preserve both.

# Subscribing

One subscription form is one descriptor type carrying all of its settings, and a mount site
names the descriptor inside `#[subscriber(..)]`. A bare `#[subscriber("key")]` opens a consumer
group over that stream key with the broker's [`default_group`](RedisBroker::default_group); a
broker with no default group refuses it at startup and the error names the key.

| Form | Descriptor | Settlement | Where a retry copy goes |
| --- | --- | --- | --- |
| Streams | [`RedisStream`] | `XACK`, a requeue re-appends a copy | the stream key |
| List, simple | [`RedisList`] | none, `AckError::Unsupported` | the list key |
| List, reliable | [`RedisList::reliable`] | `LREM` off the processing list | the list key |
| Pub/Sub channel | [`RedisPubSub`] | none, `AckError::Unsupported` | the channel |
| Pub/Sub pattern | [`RedisPubSubPattern`] | none, `AckError::Unsupported` | the mount site says |

The last column is the descriptor's own answer, and it is what the runtime's delayed retry and
its dead-letter cap publish through. Both leave through the broker's default publisher,
[`RedisDefaultPublish`], unless the mount site names another with `.out_retry(policy)`. It
writes a name the way this service reads it: a list key and a list's dead-letter destination
with `LPUSH` in that list's framing, a channel and its dead-letter destination with `PUBLISH` in
its mode and framing, anything else with `XADD`. A name one subscription reads as one type and
another writes as a second type refuses to start, naming both. A pattern reads every channel its glob matches, and a glob
is not a channel a `PUBLISH` can name, so a registration on one names the destination itself
with `.out_retry(policy).to("events.retry")` or with a publish transform that reads the channel
each delivery came in on. A registration that names neither does not start.

## Streams

A constructor picks the read mode, because the three return different sets of entries:

* [`RedisStream::new`] reads fresh entries off the tail (`XREADGROUP >`). The normal worker.
* [`RedisStream::claiming`] reads the group's pending entries idle at least `min_idle` and the
  fresh tail in one call (`XREADGROUP ... CLAIM`), the stale ones first. Redis 8.4 and later.
* [`RedisStream::reclaim`] reads only the entries another consumer fetched and never acked
  (`XAUTOCLAIM`, idle at least `min_idle`). The crash-recovery path.

Inferring the mode from a numeric parameter would be a footgun, so it is part of the constructor
name. `min_idle` has no default: set it above the longest handler runtime, or an entry a healthy
consumer is still working on is taken away and handled twice.

On Redis 8.4 and later write the claiming mode: one subscription, one consumer and one handler
cover new work and recovery alike. A claiming subscription against an older server does not
start, and the error names the subscription, the mode and both versions; recovery there is a
second subscription on the same group, a `reclaim` one beside the `new` one.

```
# mod demo {
use std::time::Duration;

use ruststream_fred::stream::prelude::*;
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u64,
}

// One handler for both sets: new orders, and the ones a worker took and never finished.
#[subscriber(RedisStream::claiming("orders", Duration::from_secs(30)).group("workers"))]
async fn handle(order: &Order) -> HandlerOutcome {
    println!("processing order {}", order.id);
    HandlerOutcome::ack()
}

// Recovery on a server older than 8.4: a reclaim subscriber beside the fresh-tail one, on the
// same group.
#[subscriber(RedisStream::reclaim("legacy", Duration::from_secs(30)).group("workers"))]
async fn recover(order: &Order) -> HandlerOutcome {
    println!("recovered order {}", order.id);
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            b.include(handle);
            b.include(recover);
        },
    )
}
# }
# fn main() {}
```

The descriptor also carries [`group`](RedisStream::group),
[`consumer`](RedisStream::consumer) (auto-generated when unnamed),
[`block`](RedisStream::block), [`start_id`](RedisStream::start_id) for where a group starts the
first time it is created ([`StreamStart`]), and [`delayed_retry`](RedisStream::delayed_retry).

## Lists

A producer `LPUSH`es an entry, consumers pop from the right, and exactly one of them gets it:
no fan-out, no replay, no groups. Simple mode (`BRPOP`, the default) settles nothing, so a crash
mid-handler loses the entry, which is at-most-once. [`RedisList::reliable`] moves the entry to a
per-consumer processing list and removes it on `ack`, which is at-least-once; a requeue returns
it to the main list and a nack without one drops it.

Reliable mode has no native idle tracking, so a consumer that dies after the move leaves its
entry stranded. [`RedisList::recovery_zset`] (with [`RedisList::min_idle`]) starts a watchdog
that returns such orphans to the main list. Without it there is no orphan recovery at all, and
Redis Streams stay the recommended durable path.

```
# mod demo {
use std::time::Duration;

use ruststream_fred::list::prelude::*;
use serde::Deserialize;

#[derive(Deserialize)]
struct Job {
    id: u64,
}

// At-least-once, with the watchdog that returns a dead consumer's entry to the queue. Set
// `min_idle` above the longest handler runtime, or a running job is recovered and repeated.
#[subscriber(
    RedisList::new("jobs")
        .reliable()
        .min_idle(Duration::from_secs(30))
        .recovery_zset("jobs.inflight")
)]
async fn run_job(job: &Job) -> HandlerOutcome {
    println!("running job {}", job.id);
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("jobs", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            b.include(run_job);
        },
    )
}
# }
# fn main() {}
```

On a cluster the queue and the processing list have to live on one hash slot, because a claim
moves the entry between them in one command. A hash tag gives them one: `RedisList::new("{jobs}")`,
whose default processing key follows the tag. A subscription whose two keys land on different
slots does not start, and the error names the tag.

[`RedisList::recovery_ttl`] expires an abandoned recovery key and has to exceed `min_idle`.
[`RedisListPublish::ttl`] re-arms a `PEXPIRE` on the list key at every publish, so a queue in use
never expires and an idle one lapses; the expiry covers the whole list, because Redis lists have
no per-entry one.

## Pub/Sub

A message reaches whichever subscribers are connected at publish time. There is no durability,
no consumer group, and no acknowledgement: `ack` and `nack` report `AckError::Unsupported`, and
a delivery nobody was connected for is gone.

Two delivery modes do not interoperate, so [`PubSubMode`] is explicit.
[`PubSubMode::Classic`] is `SUBSCRIBE` / `PUBLISH`, broadcast to every node of a cluster, and the
only option on standalone and sentinel. [`PubSubMode::Sharded`] is `SSUBSCRIBE` / `SPUBLISH`
(Redis 7 and later), slot-local so it scales across a cluster, and it has no patterns. A glob is
[`RedisPubSubPattern`], a descriptor of its own: it reads many channels and names none, so
sharded delivery has no meaning there and neither does a channel a retry copy could go to.

```
# mod demo {
use ruststream_fred::pubsub::prelude::*;
use serde::Deserialize;

#[derive(Deserialize)]
struct Event {
    kind: String,
}

#[subscriber(RedisPubSub::new("events"))]
async fn on_event(event: &Event) -> HandlerOutcome {
    println!("event: {}", event.kind);
    HandlerOutcome::ack()
}

#[subscriber(RedisPubSubPattern::new("events.*"))]
async fn on_any_event(event: &Event) -> HandlerOutcome {
    println!("matched event: {}", event.kind);
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("events", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            b.include(on_event);
            // A pattern names no channel a `PUBLISH` can reach, so this registration says where
            // a retry copy of a delivery goes. Without it the service does not start.
            b.include(on_any_event)
                .out_retry(Publish::default())
                .to("events.retry");
        },
    )
}
# }
# fn main() {}
```

A list entry and a Pub/Sub message carry their headers in a frame around the payload: a binary
frame by default, or an envelope the codec serializes when the same codec is set on both ends
([`RedisPubSub::codec`], [`RedisList::codec`] and their publish policies). Both framings carry
any bytes; the envelope writes a field whose bytes are valid UTF-8 as text and any other bytes
as themselves, which makes the value readable in a tool like `RedisInsight`. A value published
by an external client arrives as the payload with no headers.

## Settings at the mount site

The descriptor covers how a read forms; the mount chain covers what the runtime does with what
it read. A handler is registered with `b.include(h)`, and the steps chain off it:

| Step | What it does |
| --- | --- |
| `.batch(n)` | the batch size, required on a batch handler and rejected on a single one |
| `.block(d)` | this crate's step, chained after the size |
| `workers(n)`, `workers(n, by_key)` | dispatch lanes, written inside `#[subscriber(..)]` |
| `.max_attempts(n).dead_letter(name)` | the delivery cap and where a spent message goes |
| `.out_retry(policy)` | the publisher a delayed or capped copy leaves through |
| `.out_reply(policy)`, `.out(marker, policy)` | the publish positions a handler body reaches |
| `start_at(position)` | seeks the group before the first delivery, inside `#[subscriber(..)]` |

[`RedisSubscribeExt::block`] is the one Redis word among them: it sets how long a single read
waits, overriding what the descriptor named. On a stream it is the `XREADGROUP` server-side
`BLOCK` (and, in reclaim mode, the poll interval between empty `XAUTOCLAIM` scans); on a list it
is the `BRPOP` / `BLMOVE` timeout. Both default to five seconds. There is deliberately no
publisher-side twin: every publish option is one chain away from the policy the mount site
already names.

## Batches

A slice parameter makes a handler a batch handler, and the mount site names the size.

```
# mod demo {
use std::time::Duration;

use ruststream_fred::stream::prelude::*;
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u64,
}

#[subscriber(RedisStream::new("orders.bulk").group("bulk"))]
async fn handle_batch(orders: &[Order]) -> HandlerOutcome {
    println!("got {} orders", orders.len());
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            // The size is the framework's word and the block is Redis's, in that order.
            b.include(handle_batch.batch(nonzero!(16)).block(Duration::from_secs(2)));
        },
    )
}
# }
# fn main() {}
```

On a stream the size is native: it becomes the `COUNT` of the `XREADGROUP` or `XAUTOCLAIM` that
fetches the batch, the server sends at most that many entries, and the handler sees exactly what
one read returned, never an empty batch. A list and a Pub/Sub channel deliver one entry at a
time, so their subscribers assemble batches on the client through the framework's buffered
adapter and honour the same size.

## Native delivery fields

Redis carries metadata the payload does not, and a handler reads it by compile-time key off its
typed context: no hashing, no boxing, no downcast. A handler names [`context::StreamContext`] as
its context type, or binds one key with a `Ctx<K>` parameter; a batch body names
[`context::StreamBatchContext`]. A key the transport does not carry does not compile.

| Key | Value | On |
| --- | --- | --- |
| `keys::EntryId` | [`EntryId`], the `<milliseconds>-<sequence>` id read at | delivery |
| `keys::Position` | [`RedisGroupPosition`], the cursor that redelivers this entry | delivery |
| `keys::ConsumerGroup` | the group the subscription reads through | delivery and batch |
| `keys::SeekHandle` | [`RedisGroupSeeker`], the group's reposition handle | delivery and batch |
| `keys::FredPool` | `fred::clients::Pool`, the broker's connection pool | every form |
| `keys::Pipeline` | [`pipeline::RedisPipeline`], the delivery's round | a `.pipeline()` subscription |

A batch spans many deliveries, so the batch context carries only what belongs to the
subscription; an entry id or a position is read off the batch's own elements instead. Pub/Sub
has [`context::PubSubContext`] (the channel a delivery arrived on, and whether it matched
through a pattern), and a list carries nothing beyond payload and headers.

`Ctx<keys::FredPool>` hands a handler the connection pool on every form, for a command whose
answer it needs now. Alone it names [`context::PoolContext`]; beside a stream or Pub/Sub key it
reads that form's context, and goes after that key in the signature, because the first `Ctx` key
names the handler's context.

The two claiming read modes report their delivery count and idle time as the
[`DELIVERY_COUNT_HEADER`] and [`IDLE_MS_HEADER`] headers, which every transport reads the same
way. A reclaimed delivery counts itself; a claimed one counts the attempts before it, so it
reports zero for an entry read off the tail.

```
# mod demo {
use ruststream_fred::stream::prelude::*;
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u64,
}

#[subscriber(RedisStream::new("orders").group("workers"))]
async fn handle(order: &Order, ctx: &mut Context<'_, StreamContext>) -> HandlerOutcome {
    println!(
        "order {} read at {} through {}",
        order.id,
        ctx.context(keys::EntryId),
        ctx.context(keys::ConsumerGroup)
    );
    HandlerOutcome::ack()
}
# }
# fn main() {}
```

## Acknowledgement

Settlement on a stream follows the republish-retry model. `ack` is `XACK`.
`nack(requeue = true)` re-appends a copy to the same stream and then acks the original, which is
at-least-once: a crash between the two leaves a duplicate. `nack(requeue = false)` acks to drop.

A claiming subscription retries through the pending entries list instead: a retry appends
nothing and acknowledges nothing, and the subscription's own next read takes the entry back once
it has been idle `min_idle`. Nothing is duplicated and the entry keeps its id, at the price of
waiting out the threshold before the next attempt.

A simple list and both Pub/Sub forms cannot settle at all, and say so with
`AckError::Unsupported` rather than pretending. A reliable list acks by removing the entry from
its processing list.

## Delayed retry

`HandlerOutcome::retry_after(delay)` asks for a delayed redelivery. Redis Streams have no
per-message delay, so a subscription serves it in one of three ways.

The framework's own way needs nothing at the mount site: the runtime holds the copy and
publishes it back to the stream key once the delay is up, through the publisher the registration
already has. That copy is at-most-once over the delay window, since a crash before the timer
fires loses it, and its retry-count header is one higher than the original's.

A ZSET delay queue is the durable way, and it is off by default because it costs extra keys,
memory and a sweep. A delayed delivery is `ZADD`ed under its due time with its retry-count
header raised by one, and the original is `XACK`ed; the subscription sweeps the queue as it
reads and `XADD`s a due entry back onto the stream under a fresh id. One sweep happens per read, so
the `block` interval is the granularity, and a pass moves at most 128 due entries. Scores are
wall-clock epoch milliseconds, so keep clocks synced. A `ttl` cleans up an abandoned queue and
has to exceed the longest scheduled delay, or entries are dropped before they fire.

```
# mod demo {
use std::time::Duration;

use ruststream_fred::stream::prelude::*;
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u64,
}

#[subscriber(
    RedisStream::new("orders")
        .group("workers")
        .delayed_retry(DelayedRetry::DurableZset {
            key: "orders.delayed".to_owned(),
            ttl: None,
        })
)]
async fn handle(order: &Order) -> HandlerOutcome {
    if order.id == 0 {
        // Parked in the ZSET for thirty seconds instead of blocking the worker.
        return HandlerOutcome::retry_after(Duration::from_secs(30));
    }
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            // `out_retry` replaces the publisher the copies leave through: another policy,
            // another codec, a transform on the way out.
            b.include(handle).out_retry(Publish);
        },
    )
}
# }
# fn main() {}
```

The claiming mode is the third way and needs no queue, because the entry stays pending and the
next read takes it back. What it can offer is one wait, its `min_idle`, so a shorter delay
rounds up to it and a longer one is not held that long. Name a ZSET queue on a claiming
subscription to have a delay honoured as asked.

## Capping the retries

A handler that keeps asking for a retry circulates its message until an operator intervenes.
Two steps at the mount site end that, and they read the same on every transport here:
`max_attempts(n)` is how many deliveries one message gets, counting the first, and
`dead_letter(name)` is where a delivery goes once they run out, published there as it arrived,
payload and headers. A cap declared without a destination rejects the delivery instead; a
destination declared without a cap carries away every retry, on the first one.

```
# mod demo {
use ruststream_fred::stream::prelude::*;
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u64,
}

// The handler counts nothing itself.
#[subscriber(RedisStream::new("orders").group("workers"))]
async fn handle(order: &Order) -> HandlerOutcome {
    if order.id == 0 {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            // Five deliveries per message, counting the first. The fifth failure sends it to
            // "orders.dlq" instead of back to "orders".
            b.include(handle)
                .max_attempts(nonzero!(5u32))
                .dead_letter("orders.dlq");
        },
    )
}
# }
# fn main() {}
```

Redis maps none of this natively, so no descriptor here declares a broker-side retry: the
runtime applies the cap and makes the move, and the destination is a plain name on the same
broker, written with the publish of the form the delivery came from. What Redis does supply is the count the cap reads. The two read modes that claim report
the delivery count from the pending entries list, so a message a dead worker never acked counts
towards the cap without anything in this process having seen it fail; the reclaim path therefore
issues one `XPENDING` per read that found something. Every other subscription has no count of
its own, so the cap reads the framework's retry-count header, which travels on the copies the
runtime publishes, and an immediate retry under a cap becomes a copy rather than a plain nack so
that the count moves with the message. A ZSET delay queue raises that header on every entry it
replays, so a cap counts the rounds through the queue too.

The count the cap reads includes the delivery being made, so it is one ahead of
[`DELIVERY_COUNT_HEADER`] on a claiming subscription and equal to it on a reclaimed one.

## Repositioning a group

A seek is group-wide. Redis keeps one cursor per consumer group, so moving it repositions every
consumer of that group, not only the subscription that asked; the type names carry that scope.
[`StreamStart`] chooses where a group starts when it is first created, and moving a group that
already exists is the framework's `Seekable` capability, which streams implement and lists and
Pub/Sub do not.

| Constructor | Where the group resumes |
| --- | --- |
| [`RedisGroupPosition::beginning`] | the oldest entry the stream still retains |
| [`RedisGroupPosition::end`] | the tail: only entries added afterwards |
| [`RedisGroupPosition::after`] | the entry following that id, exclusive like `XGROUP SETID` |

A `start_at(..)` clause seeks the subscription before its first delivery, on every startup.
While the service runs, a handler seeks through the group's own handle, which rides the context
under `keys::SeekHandle` and is bound as a `Ctx` parameter.

```
# mod demo {
use ruststream_fred::stream::prelude::*;
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u64,
}

// Replays from the oldest retained entry on every start. The cursor belongs to the group, so
// this rewinds `auditors` as a whole.
#[subscriber(
    RedisStream::new("audit").group("auditors"),
    start_at(RedisGroupPosition::beginning())
)]
async fn replay(order: &Order) -> HandlerOutcome {
    println!("replayed order {}", order.id);
    HandlerOutcome::ack()
}

// On the producer's poison marker the worker skips the group forward to the tail instead of
// grinding through the bad region.
#[subscriber(RedisStream::new("orders").group("workers"))]
async fn handle(order: &Order, Ctx(seeker): Ctx<keys::SeekHandle>) -> HandlerOutcome {
    if order.id == 0 && seeker.seek(RedisGroupPosition::end()).await.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            b.include(replay);
            b.include(handle);
        },
    )
}
# }
# fn main() {}
```

A delivery reports its own position, and seeking to it delivers the message again and then the
entries after it, because the cursor is exclusive and the id is decremented for you. The cursor
changes as soon as the seek returns; a subscription blocked in `XREADGROUP` sees it on its next
read, within one `block` interval, and entries the old cursor selected are discarded rather than
delivered.

Three things a seek does not touch. Entries already delivered and not acknowledged stay in the
pending entries list whichever way the cursor moved, and remain reachable through the reclaim
path. Copies already sitting in a ZSET delay queue are keyed by their due time, so they are
appended when they fall due regardless of where the group reads. And a replayed entry is
delivered again, so its native delivery count grows, which means a claiming subscription counts
replays towards a declared cap while the framework's own retry-count header does not move.

# Pipelining

A `.pipeline()` subscription settles in a window, and a handler queues its own Redis commands
into the delivery's segment of that window.

```
# mod demo {
use ruststream_fred::stream::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct Order {
    id: u64,
}

#[derive(Serialize, Outgoing)]
#[outgoing(name = "receipts")]
struct Receipt {
    id: u64,
}

#[subscriber(PipelinedStream::new("orders").group("workers"), publish)]
async fn record(
    order: &Order,
    Ctx(pipeline): Ctx<keys::Pipeline>,
) -> Result<Receipt, HandlerOutcome> {
    // Queued: it runs after the handler returns, with the delivery's `XACK`.
    if pipeline.hincrby("orders:count", "seen", 1).await.is_err() {
        return Err(HandlerOutcome::retry());
    }
    Ok(Receipt { id: order.id })
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            // The reply joins the delivery's round and leaves after what the handler queued.
            b.include(record).out_reply(Publish).transform(InRound);
        },
    )
}
# }
# fn main() {}
```

`.pipeline()` is a step of [`RedisStream`], [`RedisList`] and [`RedisPubSub`]. The
`#[subscriber(..)]` attribute reads a descriptor's type off the constructor its chain starts
from, so the attribute spells a pipelined subscription with [`PipelinedStream`],
[`PipelinedList`] and [`PipelinedPubSub`], and an atomic one with [`AtomicStream`],
[`AtomicList`] and [`AtomicPubSub`]. They take the same builder steps as the descriptor, and
each form's prelude carries its two, with the [`pipeline::InRound`] transform and the
[`pipeline::Bindable`] bound.

A window flushes in three cases: the read's `COUNT` has settled, nothing is outstanding, or the
subscription stops. The flush sends every committed segment and then one pipeline of settles on
a connection of the pool other than the one the reads block on. A stream's settles leave as one
`XACK`, and a reliable list's as one `LREM` per entry. Under load that is one round trip per
fetched batch. On a trickle the window leaves as soon as the last handler of the batch returns.

What a handler queued follows the delivery's outcome:

| Outcome | The handler's commands | The settle |
| --- | --- | --- |
| `ack` | sent, before the settle | the form's own: `XACK`, `LREM` |
| `drop` | dropped | queued alone |
| `retry`, `retry_after` | dropped | the crate's own: the copy or the `ZADD`, then `XACK` |
| unsettled | dropped | none: the entry stays pending |

Pub/Sub and a simple list settle nothing, so every outcome but `ack` drops the segment. A flush
that fails is reported once on the delivery stream, and names the subscription, how many
commands and settles it carried and how many failed. On the forms that settle, the entries whose
settles failed stay pending and are delivered again.

[`pipeline::RedisPipeline`] carries `fred`'s command methods for keys, strings, hashes, lists,
sets, sorted sets, streams, functions, `PUBLISH`, and Lua through `eval` and `evalsha`, plus
`custom` for any other command. Each
one queues the command and returns `Result<(), fred::error::Error>`: `Ok` means queued, and an
error is `fred`'s refusal at queue time. The command's reply is not available inside the handler.
Take `Ctx<keys::FredPool>` for a command whose answer the handler needs now. A delivery whose
handler queues nothing costs the window no buffer. One that queues gets a `fred` pipeline of its
own, the first time it queues.

`.atomic()` follows `.pipeline()`, and makes each segment `MULTI`, the handler's commands, the
settle and `EXEC`, still inside the window's pipeline. A handler's Redis side effects and its
acknowledgement then happen together or not at all. Redis has no rollback, so a command that
fails inside `EXEC` leaves the others executed. On a cluster a transaction needs every key on
one slot, the subscription key's, which a hash tag gives: `{orders}:invoices` beside a stream
`{orders}`. A command for another slot fails inside the transaction, and Redis refuses the whole
`EXEC`.

A publish joins a delivery's segment in two ways. `pipeline.bind(&out)` binds a slot's
publisher, bounded as `Out<impl pipeline::Bindable>`, and hands the slot back with its codec and
transforms. A slot publish the handler does not bind leaves at once. The
[`pipeline::InRound`] transform on the reply position puts the reply in the round.

A batch mount's segment is the batch: the body's commands and the settles of every entry leave
together, and the commands only if every entry is acknowledged. A reliable list reads with one
pipeline of `LMOVE` and a simple one with `RPOP key count`, both waiting with their blocking pop
on an empty queue.


# Publishing

A publisher is a policy plus a live form. The policy is pure declaration, constructible
anywhere, and the runtime pairs it with the connected broker at startup, so "not connected" is
not representable on this path. Each form exports its policy under the same mount-site name,
`Publish`, through its own prelude.

| Policy | Prelude name | Sends with | Settings the policy holds |
| --- | --- | --- | --- |
| [`RedisPublish`] | `stream::Publish`, `stream::TransactionalPublish` | `XADD` | none |
| [`RedisPubSubPublish`] | `pubsub::Publish` | `PUBLISH` / `SPUBLISH` | mode, framing codec |
| [`RedisListPublish`] | `list::Publish` | `LPUSH` | framing codec, key TTL |

A registration that replies without naming a policy gets [`RedisDefaultPublish`], which writes
each name the way this service reads it and `XADD`s a name it does not read. The stream policy
is its own transactional name: a stream publisher buffers on the handle as it is, so there is no
second type to transition to.

The policy covers the publish, with one setting left to the message itself. `XADD`, `LPUSH` and
`PUBLISH` take the key or channel and the value, so a handler body usually writes
`.message(&value).publish()` and nothing else.

```
# mod demo {
use ruststream_fred::pubsub::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct Event {
    kind: String,
}

// Where a reply goes is a property of the reply type, not of the mount site.
#[derive(Serialize, Outgoing)]
#[outgoing(name = "audit")]
struct AuditEntry {
    kind: String,
}

#[subscriber(RedisPubSub::new("events"), publish)]
async fn on_event(event: &Event) -> AuditEntry {
    AuditEntry {
        kind: event.kind.clone(),
    }
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("events", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            // The reply type names where it goes; the policy names how it gets there, a
            // `PUBLISH` rather than the `XADD` the default publisher sends to a name this
            // service does not read.
            b.include(on_event).out_reply(Publish::default());
        },
    )
}
# }
# fn main() {}
```

A reply type that declares no destination takes the one the mount site gives it,
`#[subscriber("src", publish("dest"))]`. Redis has no request-reply primitive, so this crate
implements no `RequestReply` capability: nothing on the wire carries a reply address or
correlates a reply with its request, and a reply here is an ordinary publish to a name both
sides agreed on beforehand.

Outside an app, [`ConnectedRedisBroker::publisher`],
[`ConnectedRedisBroker::pubsub_publisher`] and [`ConnectedRedisBroker::list_publisher`] hand one
out directly.

## Partition keys

`workers(n, by_key)` runs a subscription on several lanes and keeps per-key order: deliveries
sharing a partition key go to the same lane. Redis has no partition of its own, so the resolved
key travels in the [`PARTITION_KEY_HEADER`] header, which is where the consuming side reads it.
Writing that header at the call site is the spelling that carries across brokers.

[`RedisPublishSteps::partition_key`] keys one message, and the step sits anywhere after the
message, so it composes with a header contract the message type declares. It writes through
[`RedisPublishOptions`], the per-message options type of every publisher in this crate, which is
also the bound that keeps the step off another broker's builder. A publish that names no step
carries no options at all and leaves the header map alone.

A handler body that sets the key is the one stated exception to "the body imports the framework
prelude alone": it imports this crate's prelude, to name the options type in the slot's bound.

```
# mod demo {
use ruststream_fred::stream::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct Order {
    id: u64,
}

#[derive(Serialize, Outgoing)]
#[outgoing(name = "orders.keyed")]
struct KeyedOrder {
    id: u64,
}

#[derive(OutSlot)]
#[publishes(KeyedOrder)]
struct Ledger;

#[subscriber(RedisStream::new("orders.in").group("workers"))]
async fn forward(
    order: &Order,
    Out(ledger): Out<impl Publisher<Options = RedisPublishOptions>, Ledger>,
) -> HandlerOutcome {
    let keyed = KeyedOrder { id: order.id };
    if ledger
        .message(&keyed)
        .partition_key("tenant-a")
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            b.include(forward).out(Ledger, Publish).build();
        },
    )
}
# }
# fn main() {}
```

## Transactions

A transaction sends a group of publishes as one `MULTI` / `EXEC` block, in publish order, so
subscribers see the whole group or none of it. The stream publisher offers both kinds the
framework defines, on standalone and sentinel; they differ in who owns the buffer. The list and
Pub/Sub publishers have none.

The borrowed kind claims the handle's own buffer: `begin_transaction` starts buffering, `commit`
flushes it, `abort` discards it, and a clone of the handle works with the same open transaction.
A second `begin_transaction` while one is open is rejected and leaves the open one untouched, as
is a `commit` or `abort` with nothing open. Its usual shape is a batch handler whose replies are
committed together, which `.transactional()` after the reply policy asks for.

The owned kind, `publisher.transaction()`, hands back a value owning its buffer: any number can
be open on one handle, settling one never touches another, and the handle keeps publishing
directly meanwhile. `commit` and `abort` consume the value, so a double commit and a publish
after settling do not compile. Dropping an unsettled transaction discards the buffer like an
abort and writes a warning. A commit that fails has consumed the transaction and its buffer is
gone: recover by redelivering the inputs, not by resubmitting the buffer.
`publisher.owned_transaction()` is the same thing over values rather than bytes, encoding each
one with the default codec.

```
# mod demo {
use ruststream::OutgoingMessage;
use ruststream_fred::stream::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

#[subscriber("orders", publish("processed"))]
async fn process(orders: &[Order]) -> Result<Vec<Order>, HandlerOutcome> {
    Ok(orders.iter().map(|order| Order { id: order.id }).collect())
}

#[ruststream::app]
fn app() -> impl App {
    let broker = RedisBroker::standalone("redis://localhost:6379").default_group("workers");
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        // One batch's replies, buffered and committed as one MULTI / EXEC block.
        b.include(process.batch(nonzero!(32)))
            .out_reply(TransactionalPublish)
            .transactional();

        // The owned kind, in the hook where a service makes its first publishes.
        b.after_startup(TransactionalPublish, async move |publisher| {
            let mut seed = publisher.transaction().await?;
            seed.publish(
                OutgoingMessage::new("processed", br#"{"id":0}"#.as_slice()),
                None,
            )
            .await?;
            seed.commit().await
        });
    })
}
# }
# fn main() {}
```

Two properties hold for both kinds, because both are `MULTI` / `EXEC`. A `MULTI` block cannot
span hash slots, so opening a transaction on a cluster returns an error instead of committing a
group that is not atomic. And there is no rollback: a command that fails at runtime inside `EXEC`
leaves the commands before it committed, which for a group of `XADD`s happens when Redis runs
out of memory or a key holds a non-stream type. A command the server refuses to queue discards
the whole block instead. Pipelining is a throughput tool and not a transaction: the list
publisher sends its `LPUSH` and `PEXPIRE` in one round trip, and they commit separately.

# The prelude

A service on one transport globs that form's prelude, [`stream::prelude`],
[`list::prelude`] or [`pubsub::prelude`]. Each carries the framework prelude, the broker, that
form's descriptors and their options, its policy under the uniform name `Publish`, the context
types it has, and only the capabilities that form actually offers. A stream also gets
`TransactionalPublish`, the seek types and the `keys` module.

[`prelude`] is for a file that mixes forms: the same contents for all three, with the policies
under their prefixed names and the [`stream`], [`list`] and [`pubsub`] modules in scope, so a
mixed file writes `stream::Publish` beside `pubsub::Publish`. There is no bare `Publish` there,
because one word would name three colliding types.

A handler file imports the framework prelude alone and names only the capability an injected
publisher needs (`Out<impl Publisher>`, `Out<impl TransactionalPublisher>`), so moving a handler
between forms changes the descriptor and not the handler. The one exception is a body that
adjusts a per-message setting: that one imports this crate's prelude, to name
[`RedisPublishOptions`] in its bound.

# The generated document

A service describes itself as an `AsyncAPI` document, and with the `asyncapi` feature on in both
crates this one fills in what only Redis knows about a channel: which structure carries its
messages, and how that structure is configured.

The specification has a `redis` binding whose four objects are all empty, and a binding key is a
closed list, so the crate writes an extension beside it instead, `x-ruststream-redis`, at
exactly the level a binding sits at. A stream subscription reports the group and consumer it
reads through, its read mode, and the idle threshold the two claiming modes claim at. A list
reports whether it acknowledges, the processing list an unfinished entry sits on, and the
framing its headers travel in. A channel reports its delivery mode and whether its address is a
glob. A publisher reports the same vocabulary from the other side: the delivery mode a publish
goes out in, the expiry a list push re-arms, the framing it writes. It also names where it
lands, in the word Redis uses for it: a stream or list publisher writes a `key`, a Pub/Sub
publisher a `channel`. That name is the destination the mount site resolved, so a reply
declared with `publish("orders.done")` reports `orders.done` whichever subscription produced
it, and a dead-letter destination is reported as a channel the service publishes to.

Every value comes from the descriptor or the policy alone, because the document is built before
anything connects. Two consequences follow. The Redis server version is not reported: the client
learns it from the handshake, which has not happened yet. And no credential can reach the
document, which matters because a document is published and shared: the server entry carries the
host and port a client dials, with the scheme, any `user:password@`, the database path and the
query stripped. A broker built with [`RedisBroker::from_pool`] reports no host at all, since the
address sits inside the pool's own configuration. There is no reply-address expression either,
because this crate routes no reply through a header.

# Testing

A test hands the harness the app `main` runs, on [`RedisBroker`], and addresses the broker by
that type. Enable this crate's `testing` feature in `[dev-dependencies]`; `TestApp` and the
assertions are the framework's:
<https://docs.rs/ruststream/latest/ruststream/testing/index.html>.

```
# #[cfg(feature = "testing")]
# mod demo {
use ruststream::testing::TestApp;
use ruststream_fred::stream::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Outgoing, Serialize)]
struct Payment {
    id: u64,
    amount: u64,
}

#[subscriber(RedisStream::new("payments").group("workers"))]
async fn process(payment: &Payment) -> HandlerOutcome {
    if payment.amount == 0 {
        return HandlerOutcome::drop();
    }
    HandlerOutcome::ack()
}

/// The app `main` runs, and the one the tests hand the harness.
pub fn app() -> impl App<State = ()> {
    RustStream::new(AppInfo::new("payments", "0.1.0"))
        .with_broker(RedisBroker::standalone("redis://localhost:6379"), |b| {
            b.include(process);
        })
}

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    // The broker connects in process: nothing dials the address.
    let tb = TestApp::start(app()).await?;
    tb.broker::<RedisBroker>()
        .message(&Payment { id: 1, amount: 100 })
        .to("payments")
        .publish()
        .await?;

    tb.broker::<RedisBroker>()
        .subscriber("payments")
        .assert_called_once()
        .settled(HandlerOutcome::ack());

    tb.shutdown().await?;
    Ok(())
}
# }
# fn main() {
#     #[cfg(feature = "testing")]
#     tokio::runtime::Builder::new_current_thread()
#         .enable_all()
#         .build()
#         .unwrap()
#         .block_on(demo::run())
#         .unwrap();
# }
```

`TestApp::start` connects the broker to a Redis server modelled inside the test process. The
connected broker is the production one: every command this crate sends, a publish, a group read,
an acknowledgement, a delay-queue sweep, reaches the model as that command and is answered as
Redis answers it. The model reads every setting off the broker: its address is parsed as
`connect` parses it, and the pool size, the default group and the cluster topology carry over.
It answers as Redis 8.4, so a [`RedisStream::claiming`] subscription mounts.

The model keeps what a service relies on:

* key types: a write of the wrong type fails with `WRONGTYPE`, and so does a read;
* consumer groups with a cursor, a pending entries list and delivery counts, `start_id`,
  `XAUTOCLAIM` and `XREADGROUP ... CLAIM` at their idle thresholds, and repositioning with
  [`RedisGroupSeeker`];
* lists, with the reliable form's processing list and its recovery set;
* Pub/Sub channels, `PSUBSCRIBE` globs and sharded channels, which keep nothing for a subscriber
  that is not there;
* sorted sets, key expiry, `MULTI` / `EXEC`, and on a cluster topology the refusal of a command
  or a transaction that spans hash slots.

Delivery follows Redis's routing. A stream entry reaches every consumer group of the stream once,
each through one of its consumers. A list element reaches one consumer. A channel message reaches
every subscription of the channel and every pattern subscription whose glob matches it.

A test's input goes where the default publisher writes its name: an `XADD`, or the `LPUSH` or
`PUBLISH` of the subscription that reads the name, in that subscription's framing.
`published::<T>(name)` reads every write to the name back, a requeue's copy and an entry a delay
queue adds back included. The model's time is the test's clock, so entry ids, idle times and the
scores of the delay queue and the list recovery stand still on a paused clock and
`tb.advance(by)` moves them.

A handler's own commands through `Ctx<keys::FredPool>` or a window run against the model too. A
command it does not model fails with an error naming it, and so do scripts and functions
(`EVAL`, `EVALSHA`, `FCALL`): a handler built on them is tested against a server.

The same test body runs against a server with `TestApp::start_live(app())`. The crate's own live
suites are gated behind per-topology environment variables, and `just test-brokers` starts the
compose stand and runs them. What only a server shows belongs there: scripts, the sentinel and
cluster topologies under failover and resharding, and authentication and TLS.

# Operations

One constructor per topology, all synchronous and free of I/O: [`RedisBroker::standalone`] takes
a URL, [`RedisBroker::cluster`] a seed list, of which one reachable node is enough,
[`RedisBroker::sentinel`] the monitored primary's name and the sentinels that watch it, and
[`RedisBroker::from_pool`] an already-built `fred` `Pool`. [`RedisBroker::pool`] sets how many
connections the broker opens.

[`RedisBroker::credentials`] sets an ACL username and password on every topology, which is the
only way to authenticate a cluster or sentinel seed list, and it overrides what a standalone URL
carries. [`RedisBroker::password`] is the legacy password-only `AUTH` with no ACL user.

TLS is three off-by-default features, additive in `fred`: `tls-rustls` (rustls on `aws-lc-rs`),
`tls-rustls-ring` (rustls on ring) and `tls-native-tls`. With one of them on,
[`RedisBroker::tls`] takes a `TlsConfig` or a `TlsConnector` on any topology, and a standalone
broker can also switch TLS on with a `rediss://` or `valkeys://` URL.

```
# #[cfg(feature = "tls-rustls")]
# mod demo {
use ruststream_fred::{RedisBroker, TlsConnector};

pub fn brokers() -> Result<(), Box<dyn std::error::Error>> {
    let _password_only = RedisBroker::sentinel("mymaster", ["10.0.0.1:26379"]).password("s3cr3t");
    let _acl = RedisBroker::cluster(["10.0.0.1:6379"]).credentials("worker", "s3cr3t");

    // System trust roots, no client certificate. The same connector works on every topology.
    let _tls = RedisBroker::cluster(["10.0.0.1:6379"]).tls(TlsConnector::default_rustls()?);
    Ok(())
}
# }
# fn main() {}
```

Two more auth features are off by default. `sentinel-auth` adds
[`RedisBroker::sentinel_credentials`] and [`RedisBroker::sentinel_password`], the credentials
that authenticate to the sentinels rather than to the data nodes. `credential-provider` adds
[`RedisBroker::credential_provider`], a callback that supplies and can rotate the username and
password on each `AUTH` or `HELLO`, and it takes precedence over static credentials.

Known limits. A transaction is unavailable on a cluster, because `MULTI` cannot span hash slots,
and a reliable list needs its two keys under one hash tag there for the same reason.
A reliable list without a recovery key has no orphan recovery. Pub/Sub loses anything published
while a subscriber is disconnected, and a simple list loses anything a crashed handler was
holding. A claiming subscription needs Redis 8.4 or later. For a setting these builders do not
reach, such as a reconnection policy or performance tuning, build a `fred` `Pool` yourself and
wrap it with [`RedisBroker::from_pool`]; the broker then reports no host to the generated
document.

# Cargo features

All off by default: this crate ships the three transports and the broker with no feature on.

* `testing`: the in-process mode of [`RedisBroker`], which the framework's `TestApp` runs an app
  on, and the framework's `testing` feature it rides.
* `asyncapi`: the document-plane hooks, the bindings and the `x-ruststream-redis` extension.
* `tls-rustls`, `tls-rustls-ring`, `tls-native-tls`: TLS, mapped onto `fred`'s backends.
* `sentinel-auth`: distinct credentials for the sentinels.
* `credential-provider`: rotating credentials through a callback.
