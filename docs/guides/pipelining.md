---
sidebar_position: 7
description: "Send a handler's Redis writes in the same round trip as its acknowledgement, only when the delivery is acknowledged, or as one MULTI block with it."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Side effects with the acknowledgement

This guide sends a handler's own Redis writes together with the acknowledgement of its delivery.

The example is an order service.
It puts the lines of each order on the warehouse's pick queue, answers with a numbered receipt, and books payments in a ledger.

## Queue a command

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/pipelining.rs#accept"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/pipelining.rs#accept"
```

</TabItem>
</Tabs>

A handler that writes to Redis and then acknowledges pays two round trips.
A crash between them applies the write without the acknowledgement, and the entry is delivered again.

`Ctx<keys::Pipeline>` gives the handler the delivery's [round](../components/pipelining/index.md), a [`RedisPipeline`](api:ruststream_fred::pipeline::RedisPipeline).
Its [command methods](../components/pipelining/commands.md) are those of `fred`: `lpush`, `hincrby`, `xadd`, `publish`, `eval` and the rest.
A call queues the command and returns at once.
`Ok` means the command is queued, not that Redis has run it.

The queued commands leave after the handler returns, and only if it acknowledges the delivery.
They go first, then the `XACK`, in one pipeline.

| Outcome | The queued commands | The settle |
| --- | --- | --- |
| `ack` | sent, before the settle | `XACK` |
| `drop` | discarded | `XACK` |
| `retry`, `retry_after` | discarded | the retry copy, then `XACK` |
| unsettled | discarded | none: the entry stays pending |

`accept` queues one push per line.
A line without a quantity drops the order, and the pushes queued for the earlier lines are discarded with it.

## Read a value now

A queued command has no answer inside the handler.
A value the handler needs now goes over the broker's connection pool, which [`Ctx<keys::FredPool>`](../components/context/fred-pool.md) gives.
`accept` numbers each receipt with `INCR` this way.
Beside `keys::Pipeline`, `keys::FredPool` comes second in the signature.

## Replies in the round

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/pipelining.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/pipelining.rs#app"
```

</TabItem>
</Tabs>

`.pipeline()` at the mount site opens the window, and the handler's commands can only be queued on a subscription mounted with it.
A handler that takes `Ctx<keys::Pipeline>` and is mounted without the step does not compile.

Without the [`InRound`](../components/pipelining/in-round.md) transform, a reply is published as soon as the handler returns, ahead of the round.
The transform on the reply puts it into the round, after the commands, so the receipt leaves only with the picks.
A publish through an `Out` slot joins the round when the handler [binds](../components/pipelining/bind.md) the slot with `pipeline.bind(out)`.

The window collects the rounds of every delivery of one read.
It leaves when the read's deliveries are all settled, so under load one round trip carries a whole fetched batch.
On a trickle it leaves as soon as the last handler returns.
A failed flush is reported once, with the subscription and the number of commands and settles it carried.
The entries whose `XACK` failed stay pending and are delivered again.

A batch handler takes the round from its context, `ctx.context(keys::Pipeline)`, and its commands leave only if every entry of the batch is acknowledged ([Flush](../components/pipelining/flush.md)).

## Atomic segments

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/pipelining.rs#capture"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/pipelining.rs#capture"
```

</TabItem>
</Tabs>

A pipeline is not a transaction: a command can succeed while the `XACK` after it fails.
[`.atomic()`](../components/pipelining/atomic.md) after `.pipeline()` wraps each delivery's commands and its settle in `MULTI` and `EXEC`.
The ledger entry, the balance and the acknowledgement of a payment then happen together or not at all.

Redis has no rollback.
A command that fails inside `EXEC`, such as an `HINCRBY` on a key of another type, leaves the others applied.

On a [cluster](cluster.md) a `MULTI` block runs on one hash slot.
The stream is `{payments}` and the handler's keys are `{payments}:ledger` and `{payments}:balances`: the hash tag in braces puts them all on the stream's slot.
The stream's name has braces, so the payment type declares no destination and a producer names the stream when it publishes.

## Lua scripts

`eval` and `evalsha` queue a Lua script like any other command, and the script runs in the round with the settle.
A script runs atomically on the server, so it suits a check and a write that must not be split.
The [in-process Redis](../components/testing/index.md) runs no scripts, so a test of a handler that queues one runs against a server.

## Testing

The tests in the example files publish an order, a dropped order and a payment in process.
They read back the picks, the receipt and the ledger entry, and check that a dropped order sends none of what it queued.
