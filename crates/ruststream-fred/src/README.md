Redis and Valkey broker for the [RustStream](https://github.com/powersemmi/ruststream) messaging
framework, backed by the [`fred`](https://docs.rs/fred) client.

Three Redis transports sit behind one broker. Redis Streams is the durable one, a log read
through consumer groups with acknowledgement, redelivery and crash recovery. A Redis list is a
work queue whose every entry goes to one consumer. Pub/Sub delivers to the subscribers connected
when a message is published. Four topologies serve all three: a standalone server, a cluster, a
sentinel set, and a `fred` `Pool` a service built for itself.

# A service

A handler is an `async fn` over the decoded payload, mounted on a broker inside the application
object, and the attribute writes the `main`:

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
    tracing::info!(order.id, "accepted");
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

`cargo run -- run` starts it. [`RedisBroker::standalone`] only records the address; the runtime
connects the broker at startup and shuts it down when the service stops. A bare key in
`#[subscriber]` reads that stream through the broker's default consumer group.

# Where things are

The [guides][guides] take a Redis service from installation to production, in this order:
[getting started][g-start], [choosing a transport][g-transport],
[consumer groups and crash recovery][g-groups], [work queues on lists][g-queues],
[retries, delays and dead letters][g-retries], [fan-out with Pub/Sub][g-fan-out],
[side effects with the acknowledgement][g-pipelining], [transactions][g-transactions],
[replay and seeking][g-replay], [Redis Cluster][g-cluster], [running in production][g-production],
[testing][g-testing] and [interoperating with other clients][g-interop]. The
[architecture][architecture] pages show how Redis delivers messages and where the crate sits
between `fred` and the runtime, and the [benchmarks][benchmarks] what it costs.

Each topic below links to its component page on the site, which shows it in a service; the
item's own documentation keeps the detail.

## Broker

- [RedisBroker][c-broker]: [`RedisBroker`], connected into [`ConnectedRedisBroker`] and closed
  into [`ClosedRedisBroker`].
- [Cluster][c-cluster] and [Sentinel][c-sentinel]: [`RedisBroker::cluster`] and
  [`RedisBroker::sentinel`].
- [from_pool][c-from-pool]: [`RedisBroker::from_pool`] wraps a `fred` `Pool` the service built.
- [Pool][c-pool]: [`RedisBroker::pool`], the connections publishes and settlements share.
- [default_group][c-default-group]: [`RedisBroker::default_group`], the group a bare key reads
  through.
- [Authentication][c-authentication]: [`RedisBroker::credentials`] and
  [`RedisBroker::password`].
- [TLS][c-tls]: [`RedisBroker::tls`] with the `tls-rustls`, `tls-rustls-ring` or
  `tls-native-tls` feature.
- [sentinel_credentials][c-sentinel-credentials]: [`RedisBroker::sentinel_credentials`] and
  [`RedisBroker::sentinel_password`] (feature `sentinel-auth`).
- [credential_provider][c-credential-provider]: [`RedisBroker::credential_provider`], rotating
  credentials (feature `credential-provider`).
- [Shutdown][c-shutdown]: [`ClosedRedisBroker`], and [`RedisError::ShutDown`] for a handle used
  afterwards.
- [Errors][c-errors]: [`RedisError`].
- [Prelude][c-prelude]: [`stream::prelude`], [`list::prelude`], [`pubsub::prelude`] and the
  mixed-form [`prelude`].

## Subscriptions

- [Subscriptions][c-subscriptions]: the descriptors, what each settles and where its retry copy
  goes.
- [block][c-block]: [`RedisSubscribeExt::block`], how long one read waits on the server.
- [Batches][c-batches]: a slice handler, a native `COUNT` on a stream.
- [Envelopes][c-envelopes]: [`envelope`], how the headers travel beside the payload, set with
  [`RedisBroker::envelope`] and overridden per descriptor and policy.

## Streams

- [RedisStream][c-streams]: [`RedisStream`], a stream read through a consumer group.
- [reclaim][c-reclaim]: [`RedisStream::reclaim`], the entries a dead consumer never acked.
- [claiming][c-claiming]: [`RedisStream::claiming`], pending and fresh entries in one read
  (Redis 8.4).
- [StreamStart][c-stream-start]: [`StreamStart`], where a new group starts.

## Positions

- [RedisGroupPosition][c-positions]: [`RedisGroupPosition`], where a consumer group resumes.
- [start_at][c-start-at]: a group moved to a position on every startup.
- [Seek][c-seek]: [`RedisGroupSeeker`], the group moved from a handler.
- [EntryId][c-entry-id]: [`EntryId`], a stream entry's id.

## Lists

- [Lists][c-lists]: [`RedisList`], a work queue.
- [reliable][c-reliable]: [`RedisList::reliable`], at-least-once through a processing list.
- [Recovery][c-recovery]: [`RedisList::recovery_zset`], the watchdog for a dead consumer's
  entries.
- [processing][c-processing]: [`RedisList::processing`], and the hash tag a cluster needs.

## Pub/Sub

- [PubSub][c-pubsub]: [`RedisPubSub`], one channel.
- [RedisPubSubPattern][c-pattern]: [`RedisPubSubPattern`], every channel a glob matches.
- [PubSubMode][c-pubsub-mode]: [`PubSubMode`], classic or sharded delivery.
- [buffer][c-buffer]: [`RedisPubSub::buffer`], what a busy subscription holds.

## Acknowledgement

- [Acknowledgement][c-acknowledgement]: what `ack` and `nack` do on each form.
- [retry_after][c-retry-after]: a delayed retry through the runtime's copy.
- [DelayedRetry][c-delayed-retry]: [`DelayedRetry`], a delay queue that survives a restart.
- [dead_letter][c-dead-letter]: the retry cap and where a spent message goes.
- [Counts][c-counts]: [`DELIVERY_COUNT_HEADER`] and [`IDLE_MS_HEADER`] from the claiming modes.
- [out_retry][c-out-retry]: where a retry copy goes, named at the mount site.

## Context

- [StreamContext][c-context]: [`context::StreamContext`] and the keys in [`context::keys`].
- [ConsumerGroup][c-consumer-group]: [`context::keys::ConsumerGroup`].
- [FredPool][c-fred-pool]: [`context::keys::FredPool`], the connection pool in a handler.
- [StreamBatchContext][c-batch-context]: [`context::StreamBatchContext`], a batch handler's
  context.
- [PubSubContext][c-pubsub-context]: [`context::PubSubContext`], the channel a message came on.

## Pipelining

- [pipeline][c-pipeline]: [`pipeline::RedisPipelineSteps`], a window that settles a
  subscription's reads together with its handlers' commands.
- [atomic][c-atomic]: one `MULTI` / `EXEC` per delivery.
- [Commands][c-commands]: [`pipeline::RedisPipeline`], the commands a handler queues.
- [bind][c-bind]: [`pipeline::Bindable`], a slot's publishes in the delivery's round.
- [InRound][c-in-round]: [`pipeline::InRound`], the reply in the delivery's round.
- [Flush][c-flush]: when a window leaves, and what each outcome does to it.

## Publishing

- [RedisPublish][c-publishing]: [`RedisPublish`](struct@RedisPublish), `XADD` to a stream, and its live
  [`RedisPublisher`].
- [RedisDefaultPublish][c-default]: [`RedisDefaultPublish`], each name written the way the
  service reads it.
- [RedisPubSubPublish][c-pubsub-publish]: [`RedisPubSubPublish`], `PUBLISH` or `SPUBLISH`.
- [RedisListPublish][c-list-publish]: [`RedisListPublish`], `LPUSH` with an optional key expiry.
- [partition_key][c-partition-key]: [`RedisPublishSteps::partition_key`] and
  [`RedisPublishOptions`].

## Transactions

- [Transactions][c-transactions]: [`RedisTransactionalPublish`](struct@RedisTransactionalPublish), one `MULTI` / `EXEC` block of
  publishes on a stream publisher; a cluster refuses it at startup.
- [transaction][c-transaction]: [`RedisTransaction`], a transaction that owns its buffer.
- [owned_transaction][c-owned-transaction]: the same over values.

## Tooling

- [AsyncAPI][c-asyncapi]: the `x-ruststream-redis` extension of the generated document, with its
  [server][c-server], [channels][c-channels] and [operations][c-operations] (feature `asyncapi`).
- [Testing][c-testing]: the in-process Redis
  [`TestApp`](https://docs.rs/ruststream/latest/ruststream/testing/struct.TestApp.html) connects
  [`RedisBroker`] to (feature `testing`), and [Live][c-live] runs the same test against a server.

# Cargo features

All off by default: the crate ships the three transports and the broker with no feature on.
`testing` adds the in-process mode `TestApp::start` connects the broker through, and `asyncapi`
the Redis extension of the generated document. `tls-rustls`, `tls-rustls-ring` and
`tls-native-tls` map onto `fred`'s TLS backends. `sentinel-auth` adds credentials of their own
for the sentinels, and `credential-provider` rotating credentials through a callback.

[guides]: https://powersemmi.github.io/ruststream-fred/latest/
[g-start]: https://powersemmi.github.io/ruststream-fred/latest/guides/getting-started/
[g-transport]: https://powersemmi.github.io/ruststream-fred/latest/guides/choosing-a-transport/
[g-groups]: https://powersemmi.github.io/ruststream-fred/latest/guides/consumer-groups/
[g-queues]: https://powersemmi.github.io/ruststream-fred/latest/guides/work-queues/
[g-retries]: https://powersemmi.github.io/ruststream-fred/latest/guides/retries/
[g-fan-out]: https://powersemmi.github.io/ruststream-fred/latest/guides/fan-out/
[g-pipelining]: https://powersemmi.github.io/ruststream-fred/latest/guides/pipelining/
[g-transactions]: https://powersemmi.github.io/ruststream-fred/latest/guides/transactions/
[g-replay]: https://powersemmi.github.io/ruststream-fred/latest/guides/replay/
[g-cluster]: https://powersemmi.github.io/ruststream-fred/latest/guides/cluster/
[g-production]: https://powersemmi.github.io/ruststream-fred/latest/guides/production/
[g-testing]: https://powersemmi.github.io/ruststream-fred/latest/guides/testing/
[g-interop]: https://powersemmi.github.io/ruststream-fred/latest/guides/interop/
[architecture]: https://powersemmi.github.io/ruststream-fred/latest/architecture/
[benchmarks]: https://powersemmi.github.io/ruststream-fred/latest/architecture/benchmarks/
[c-broker]: https://powersemmi.github.io/ruststream-fred/latest/components/broker/
[c-cluster]: https://powersemmi.github.io/ruststream-fred/latest/components/broker/cluster/
[c-sentinel]: https://powersemmi.github.io/ruststream-fred/latest/components/broker/sentinel/
[c-from-pool]: https://powersemmi.github.io/ruststream-fred/latest/components/broker/from-pool/
[c-pool]: https://powersemmi.github.io/ruststream-fred/latest/components/broker/pool/
[c-default-group]: https://powersemmi.github.io/ruststream-fred/latest/components/broker/default-group/
[c-authentication]: https://powersemmi.github.io/ruststream-fred/latest/components/broker/authentication/
[c-tls]: https://powersemmi.github.io/ruststream-fred/latest/components/broker/tls/
[c-sentinel-credentials]: https://powersemmi.github.io/ruststream-fred/latest/components/broker/sentinel-credentials/
[c-credential-provider]: https://powersemmi.github.io/ruststream-fred/latest/components/broker/credential-provider/
[c-shutdown]: https://powersemmi.github.io/ruststream-fred/latest/components/broker/shutdown/
[c-errors]: https://powersemmi.github.io/ruststream-fred/latest/components/broker/errors/
[c-prelude]: https://powersemmi.github.io/ruststream-fred/latest/components/broker/prelude/
[c-subscriptions]: https://powersemmi.github.io/ruststream-fred/latest/components/subscriptions/
[c-block]: https://powersemmi.github.io/ruststream-fred/latest/components/subscriptions/block/
[c-batches]: https://powersemmi.github.io/ruststream-fred/latest/components/subscriptions/batches/
[c-envelopes]: https://powersemmi.github.io/ruststream-fred/latest/components/subscriptions/envelopes/
[c-streams]: https://powersemmi.github.io/ruststream-fred/latest/components/streams/
[c-reclaim]: https://powersemmi.github.io/ruststream-fred/latest/components/streams/reclaim/
[c-claiming]: https://powersemmi.github.io/ruststream-fred/latest/components/streams/claiming/
[c-stream-start]: https://powersemmi.github.io/ruststream-fred/latest/components/streams/stream-start/
[c-positions]: https://powersemmi.github.io/ruststream-fred/latest/components/positions/
[c-start-at]: https://powersemmi.github.io/ruststream-fred/latest/components/positions/start-at/
[c-seek]: https://powersemmi.github.io/ruststream-fred/latest/components/positions/seek/
[c-entry-id]: https://powersemmi.github.io/ruststream-fred/latest/components/positions/entry-id/
[c-lists]: https://powersemmi.github.io/ruststream-fred/latest/components/lists/
[c-reliable]: https://powersemmi.github.io/ruststream-fred/latest/components/lists/reliable/
[c-recovery]: https://powersemmi.github.io/ruststream-fred/latest/components/lists/recovery/
[c-processing]: https://powersemmi.github.io/ruststream-fred/latest/components/lists/processing/
[c-pubsub]: https://powersemmi.github.io/ruststream-fred/latest/components/pubsub/
[c-pattern]: https://powersemmi.github.io/ruststream-fred/latest/components/pubsub/pattern/
[c-pubsub-mode]: https://powersemmi.github.io/ruststream-fred/latest/components/pubsub/pubsub-mode/
[c-buffer]: https://powersemmi.github.io/ruststream-fred/latest/components/pubsub/buffer/
[c-acknowledgement]: https://powersemmi.github.io/ruststream-fred/latest/components/acknowledgement/
[c-retry-after]: https://powersemmi.github.io/ruststream-fred/latest/components/acknowledgement/retry-after/
[c-delayed-retry]: https://powersemmi.github.io/ruststream-fred/latest/components/acknowledgement/delayed-retry/
[c-dead-letter]: https://powersemmi.github.io/ruststream-fred/latest/components/acknowledgement/dead-letter/
[c-counts]: https://powersemmi.github.io/ruststream-fred/latest/components/acknowledgement/counts/
[c-out-retry]: https://powersemmi.github.io/ruststream-fred/latest/components/acknowledgement/out-retry/
[c-context]: https://powersemmi.github.io/ruststream-fred/latest/components/context/
[c-consumer-group]: https://powersemmi.github.io/ruststream-fred/latest/components/context/consumer-group/
[c-fred-pool]: https://powersemmi.github.io/ruststream-fred/latest/components/context/fred-pool/
[c-batch-context]: https://powersemmi.github.io/ruststream-fred/latest/components/context/batch-context/
[c-pubsub-context]: https://powersemmi.github.io/ruststream-fred/latest/components/context/pubsub-context/
[c-pipeline]: https://powersemmi.github.io/ruststream-fred/latest/components/pipelining/
[c-atomic]: https://powersemmi.github.io/ruststream-fred/latest/components/pipelining/atomic/
[c-commands]: https://powersemmi.github.io/ruststream-fred/latest/components/pipelining/commands/
[c-bind]: https://powersemmi.github.io/ruststream-fred/latest/components/pipelining/bind/
[c-in-round]: https://powersemmi.github.io/ruststream-fred/latest/components/pipelining/in-round/
[c-flush]: https://powersemmi.github.io/ruststream-fred/latest/components/pipelining/flush/
[c-publishing]: https://powersemmi.github.io/ruststream-fred/latest/components/publishing/
[c-default]: https://powersemmi.github.io/ruststream-fred/latest/components/publishing/default/
[c-pubsub-publish]: https://powersemmi.github.io/ruststream-fred/latest/components/publishing/pubsub-publish/
[c-list-publish]: https://powersemmi.github.io/ruststream-fred/latest/components/publishing/list-publish/
[c-partition-key]: https://powersemmi.github.io/ruststream-fred/latest/components/publishing/partition-key/
[c-transactions]: https://powersemmi.github.io/ruststream-fred/latest/components/transactions/
[c-transaction]: https://powersemmi.github.io/ruststream-fred/latest/components/transactions/transaction/
[c-owned-transaction]: https://powersemmi.github.io/ruststream-fred/latest/components/transactions/owned-transaction/
[c-asyncapi]: https://powersemmi.github.io/ruststream-fred/latest/components/asyncapi/
[c-server]: https://powersemmi.github.io/ruststream-fred/latest/components/asyncapi/server/
[c-channels]: https://powersemmi.github.io/ruststream-fred/latest/components/asyncapi/channels/
[c-operations]: https://powersemmi.github.io/ruststream-fred/latest/components/asyncapi/operations/
[c-testing]: https://powersemmi.github.io/ruststream-fred/latest/components/testing/
[c-live]: https://powersemmi.github.io/ruststream-fred/latest/components/testing/live/
