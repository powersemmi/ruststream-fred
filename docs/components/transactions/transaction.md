---
sidebar_position: 1
title: transaction
description: "transaction hands back a value that owns its buffer, so several can be open at once and a double commit does not compile."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# transaction

[`transaction`](api:ruststream::OwnedTransactions::transaction) hands back a value that owns its buffer, so several can be open at once and a double commit does not compile.

This is the owned kind, the core's [`OwnedTransactions`](api:ruststream::OwnedTransactions).
A handler takes its slot as `Out<impl OwnedTransactions, Slot>` and calls `transaction()` on it.
The value it gets back publishes like the slot, `txn.message(&value).publish()`, and admits the types the slot admits.
Each call opens a buffer of its own, so the workers of a `workers(n)` subscription never share one, and the slot keeps publishing directly meanwhile.
`commit` sends the buffer as one `MULTI` / `EXEC` block; `abort` discards it.
Both consume the value, so a second commit or a publish after settling does not compile.

Dropping a transaction that was neither committed nor aborted discards its buffer, as an abort does, and writes a warning.
A commit that fails has consumed the transaction, and its buffer is gone.
The recovery is to redeliver the input, which a `retry` outcome does, not to send the buffer again.
The live transaction is a [`RedisTransaction`](api:ruststream_fred::RedisTransaction), with the same limits as the [borrowed kind](./index.md): standalone and sentinel only, no rollback.
The slot's policy is `TransactionalPublish`, so a cluster refuses it at startup.

## Example

An order ships one entry per item in one block, on four workers at once; an item without a name aborts the transaction, which discards the entries already buffered.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_transactions_transaction.rs#transaction"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_transactions_transaction.rs#transaction"
```

</TabItem>
</Tabs>
