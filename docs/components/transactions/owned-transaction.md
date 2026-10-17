---
sidebar_position: 2
title: owned_transaction
description: "owned_transaction is the same buffer over values, encoding each one with the default codec."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# owned_transaction

[`owned_transaction`](api:ruststream::runtime::PublishExt::owned_transaction) is the same buffer over values, encoding each one with the default codec.

It is the owned kind on a live publisher outside a handler, where no slot carries a codec: the publisher an `after_startup` hook is handed, for one.
`publisher.owned_transaction()` returns a [`TypedTransaction`](api:ruststream::runtime::TypedTransaction) that owns its buffer.
`publish(name, &value)` encodes the value and buffers it; `commit` sends the buffer as one `MULTI` / `EXEC` block, and `abort` discards it.
As with [transaction](./transaction.md), any number can be open on one publisher, and `commit` and `abort` consume the value.

[`after_startup`](api:ruststream::runtime::BrokerScope::after_startup) runs its hook once the broker is connected and the subscriptions are open, with the policy it names paired into a live publisher.
That makes it the place for a service's first publishes, such as seed data that must arrive whole.
A hook that returns an error fails the startup.
On a cluster `TransactionalPublish` refuses to pair, so the service does not start and seeds nothing.

## Example

The service writes its opening prices as one block once it is connected; the catalog reader sees both or neither.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_transactions_owned.rs#owned_transaction"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_transactions_owned.rs#owned_transaction"
```

</TabItem>
</Tabs>
