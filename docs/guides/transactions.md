---
sidebar_position: 8
description: "Publish a group of messages to Redis streams all or nothing with MULTI and EXEC: a batch's replies, or a transaction a handler owns."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Transactions

This guide publishes groups of messages to Redis streams all or nothing.

The example is an order service.
It confirms orders in batches and ships each packed parcel as one shipment per item.

## A batch's replies in one block

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/transactions.rs#confirm"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/transactions.rs#confirm"
```

</TabItem>
</Tabs>

A transaction sends its publishes as one `MULTI` and `EXEC` block, in publish order.
A reader of `confirmations` sees every confirmation of a batch, or none of them.

`confirm` is a batch handler: it takes a slice and returns one confirmation per order.
An `Err` publishes nothing and settles the whole batch with its outcome.

## The mount

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/transactions.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/transactions.rs#app"
```

</TabItem>
</Tabs>

`.out_reply(TransactionalPublish).transactional()` asks for the [borrowed kind](../components/transactions/index.md).
The runtime opens a transaction on the reply publisher, publishes the batch's replies into it and commits.
`TransactionalPublish` is another name of the stream publish policy, [`RedisPublish`](api:ruststream_fred::RedisPublish), which sends with `XADD`.

The size of the batch is the `COUNT` of the read that fetches it, so one transaction holds at most 32 confirmations here.

## A transaction the handler owns

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/transactions.rs#ship"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/transactions.rs#ship"
```

</TabItem>
</Tabs>

[`transaction()`](../components/transactions/transaction.md) on an `Out` slot returns a value that owns its buffer.
Its publishes go into the buffer, `commit` sends the buffer as one block, and `abort` discards it.
`commit` and `abort` consume the value, so a second commit or a publish after the end does not compile.
Several owned transactions can be open on one publisher at once, and settling one never touches another.
A transaction dropped without either is discarded like an abort, with a warning in the log.

[`owned_transaction()`](../components/transactions/owned-transaction.md) is the same over values: each publish takes a destination and a value and encodes it with the default codec.
It suits a startup hook, such as `after_startup`, that seeds a stream.

## What "no rollback" means

Redis queues the commands of a block and runs them together; it does not undo them.
A command the server refuses to queue, such as one with a wrong number of arguments, discards the whole block, and `commit` returns an error.
A command that fails while the block runs leaves the commands before it applied.
For a group of `XADD`s that happens when the server runs out of memory or a key holds another type.

A failed commit has consumed its transaction, and the buffer is gone.
The handler returns `retry`, and the delivery comes again and builds a new transaction.

## Where transactions run

Transactions run on a standalone server and on a sentinel deployment.
A `MULTI` block cannot span hash slots, so on a [cluster](cluster.md) opening either kind returns an error.
The list and Pub/Sub publishers have no transactions.

A transaction covers publishes.
On the consuming side, [`.pipeline().atomic()`](pipelining.md#atomic-segments) puts a handler's own Redis commands and the acknowledgement of its delivery in one block.

## Testing

The tests in the example files run in process, where the [in-process Redis](../components/testing/index.md) runs `MULTI` and `EXEC`.
They check that a batch's confirmations and a parcel's shipments arrive together, and that a dropped batch and an aborted parcel publish nothing.
