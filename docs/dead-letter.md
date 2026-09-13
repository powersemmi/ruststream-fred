# Capping the retries

A handler that keeps asking for a retry circulates its message until an operator intervenes. Two
steps at the mount site end that, and they read the same on every transport this crate offers:

```rust
--8<-- "crates/ruststream-fred/examples/fred_dead_letter.rs:app"
```

`max_attempts(n)` is how many deliveries one message gets, counting the first. `dead_letter(name)`
is where a delivery goes once they run out: it is published there as it arrived, payload and
headers. A cap declared without a destination rejects the delivery instead. A destination declared
without a cap carries away every retry, on the first one.

The handler counts nothing itself:

```rust
--8<-- "crates/ruststream-fred/examples/fred_dead_letter.rs:handler"
```

A reliable list reads the same, with a list key as the destination:

```rust
--8<-- "crates/ruststream-fred/examples/fred_list_dead_letter.rs:handler"
```

## Which count the cap reads

The two stream read modes that claim, `reclaim` and `claiming`, report the delivery count Redis
keeps in the pending entries list. A message fetched by a worker that died counts towards the cap
when it is claimed back, without anything in this process having seen it fail.

Every other subscription has no count of its own, so the cap is read from the framework's
retry-count header, which travels on the copies the runtime publishes. An immediate retry under a
cap is then a copy rather than a plain `nack`, so the count moves with the message.

A claiming delivery also carries `DELIVERY_COUNT_HEADER` and `IDLE_MS_HEADER`, which report the
pending entries list as it stood before this delivery. The count the cap reads adds the delivery
being made, so it is one ahead of that header on a claiming subscription and equal to it on a
reclaimed one.

## Where a retry copy goes

A copy is published by this process whenever Redis has no redelivery of its own to use, so every
descriptor says where its copies go.

| Subscription | Where a copy is published |
|---|---|
| `RedisStream`, any read mode | the stream key: an `XADD` there is read by the consumer group |
| `RedisList` | the list key |
| `RedisPubSub` | the channel |
| `RedisPubSubPattern` | nowhere the descriptor can name: you name it |

A pattern reads every channel its glob matches, and a glob is not a channel a `PUBLISH` can name,
so a registration on one names the destination itself with `.out_retry(policy).to("events.retry")`
or with a publish transform that reads the channel each delivery came in on. A registration that
names neither refuses to start.

A `reclaim` subscription is the one place where the copy reaches the group rather than the
subscription that made it: `XAUTOCLAIM` only ever hands out entries already pending on another
consumer, so a fresh entry is read by the fresh-tail worker beside it. That is the topology the
mode is written for.

The destination a cap names is a plain name on the same broker, and the generated
[AsyncAPI document](asyncapi.md) reports it as a channel the service publishes to.
