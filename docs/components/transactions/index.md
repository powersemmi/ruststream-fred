---
title: Transactions
description: "A stream publisher sends a group of publishes as one MULTI and EXEC block; a batch's replies commit together with .transactional()."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Transactions

A stream publisher sends a group of publishes as one `MULTI` and `EXEC` block; a batch's replies commit together with [`.transactional()`](api:ruststream::runtime::Mounting::transactional).

The stream publisher, [`RedisPublisher`](api:ruststream_fred::RedisPublisher), buffers the publishes of a transaction and sends them on commit, in publish order.
Subscribers then see the whole group or none of it.
It offers both kinds of transaction the framework defines.
A mount that publishes in transactions names [`RedisTransactionalPublish`](api:ruststream_fred::RedisTransactionalPublish), `TransactionalPublish` in the stream prelude, which pairs into the same publisher.
This page is the borrowed kind; [transaction](./transaction.md) and [owned_transaction](./owned-transaction.md) are the owned kind.

The borrowed kind is the core's [`TransactionalPublisher`](api:ruststream::TransactionalPublisher): one transaction on the publisher handle.
`begin_transaction` starts buffering, `commit` flushes the buffer, `abort` discards it, and a clone of the handle works with the same open transaction.
A second `begin_transaction` while one is open returns [`RedisError::TransactionBusy`](api:ruststream_fred::RedisError::TransactionBusy) and leaves the open one alone.
A `commit` or an `abort` with nothing open returns [`RedisError::NoTransaction`](api:ruststream_fred::RedisError::NoTransaction).
Its usual shape is a batch handler whose replies commit together: `.transactional()` after `.out_reply(TransactionalPublish)` asks for it, and a policy without transactions does not compile there.

Transactions run on standalone and sentinel deployments.
A `MULTI` block cannot span hash slots, so on a cluster `TransactionalPublish` refuses to pair and the service does not start.
The error is [`RedisError::InvalidOptions`](api:ruststream_fred::RedisError::InvalidOptions), raised before the service reads its first batch.
Redis does not roll back: a command that fails inside `EXEC` leaves the commands before it committed.
For a group of `XADD`s that happens when Redis runs out of memory or a key holds another type.
A command the server refuses to queue discards the whole block instead.
The list and Pub/Sub publishers send each message on its own.

## Example

Each batch's replies reach `processed` in one block.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_transactions.rs#transactional"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_transactions.rs#transactional"
```

</TabItem>
</Tabs>
