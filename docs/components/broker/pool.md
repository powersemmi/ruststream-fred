---
sidebar_position: 4
title: Pool
description: "pool sets how many connections publishes, settlements and handler commands share; each subscription reads on a connection of its own."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Pool

[`pool`](api:ruststream_fred::RedisBroker::pool) sets how many connections publishes, settlements and handler commands share; each subscription reads on a connection of its own.

The pool holds 4 connections unless the service sets another number.
Replies, acknowledgements, retries and the commands a handler sends through [`FredPool`](../context/fred-pool.md) take turns on them.
Each stream and list subscription reads on one more connection, and each Pub/Sub subscription listens on one.
So a blocking read never holds up a publish, and a service opens the pool plus one connection per subscription.

A pool of zero connections fails startup.
A [`TestApp`](../testing/index.md) test keeps the pool size, and refuses zero the same way.

## Example

The service answers each order with a receipt through a pool of sixteen connections.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_broker_pool.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_broker_pool.rs#app"
```

</TabItem>
</Tabs>
