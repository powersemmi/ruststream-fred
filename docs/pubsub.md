# Pub/Sub

Pub/Sub is fire-and-forget: a message reaches the subscribers connected at that moment, and `ack`
and `nack` report `Unsupported`.

A Pub/Sub service imports `ruststream_fred::pubsub::prelude::*`: the descriptor, its mode, and this
form's publish policy under the name `Publish`.

A `RedisPubSub` descriptor names the channel and the mode. Classic delivery reaches every node of a
cluster:

```rust
--8<-- "crates/ruststream-fred/examples/fred_pubsub.rs:classic"
```

A glob is `RedisPubSubPattern`, a descriptor of its own, because a pattern subscription differs in
more than a flag: it reads every channel the glob matches and has none of its own to be reached at.

```rust
--8<-- "crates/ruststream-fred/examples/fred_pubsub.rs:pattern"
```

That is why a registration on a pattern says where a retry copy of a delivery goes, either with
`.out_retry(policy).to("name")` or with a publish transform that reads the channel the delivery came
in on. A registration that names neither refuses to start. See
[Capping the retries](dead-letter.md#where-a-retry-copy-goes).

Sharded delivery (`SSUBSCRIBE`, Redis 7+) stays slot-local, so it scales across a cluster and takes
no patterns. `RedisPubSubPattern` carries no mode, so the combination is not expressible;
`.mode(PubSubMode::Sharded)` selects sharded delivery on a channel subscription:

```rust
--8<-- "crates/ruststream-fred/examples/fred_pubsub.rs:sharded"
```

One service runs classic Pub/Sub on a standalone server and sharded Pub/Sub on a cluster at the same
time; each handler mounts on its own broker:

```rust
--8<-- "crates/ruststream-fred/examples/fred_pubsub.rs:app"
```

You name this form's policy where the handler is mounted: `.out_reply(Publish)`, with
`.mode(PubSubMode::Sharded)` to match a sharded subscriber. The position it binds is the value the
handler returns. That policy sends the reply with `PUBLISH`, not with the `XADD` of the broker's
default publisher.

The channel the reply goes to comes from the reply type: `AuditEntry` above declares `audit`. A reply
type that declares no channel goes where the subscriber's `publish("..")` names.

Pub/Sub delivers one message at a time, so the subscriber assembles batches itself. A batch handler
names its size with `batch(n)` where it is mounted and never sees a longer batch (see
[Batches](streams.md#batches)).

A publish frames the headers together with the payload. The default frame is binary. Setting the
same codec on the subscriber and the publisher (`.codec(JsonCodec)`) switches to a
`{headers, payload}` envelope the codec serializes, which makes the value readable in tools like
RedisInsight. Both frames carry any bytes unchanged: in the envelope a field that is valid UTF-8 is
written as text, and any other bytes as themselves. A value published by an external client arrives
as the payload with no headers.
