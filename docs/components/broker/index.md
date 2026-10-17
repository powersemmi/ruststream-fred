---
title: RedisBroker
description: "RedisBroker connects a service to Redis or Valkey; standalone only records the URL, and the runtime connects it at startup."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# RedisBroker

[`RedisBroker`](api:ruststream_fred::RedisBroker) connects a service to Redis or Valkey; [`standalone`](api:ruststream_fred::RedisBroker::standalone) only records the URL, and the runtime connects it at startup.

`standalone` takes the URL of one server, such as `redis://localhost:6379`.
The URL may carry a user and password (`redis://user:pass@host:6379`) and a database number (`/2`).
A `rediss://` or `valkeys://` URL switches [TLS](./tls.md) on.
The constructor does no I/O, so the whole service fits the synchronous `#[ruststream::app]` builder.
A [cluster](./cluster.md), a [Sentinel deployment](./sentinel.md) and a [pool the service built](./from-pool.md) have constructors of their own.
The settings that follow are builder steps too: the [pool size](./pool.md), the [default consumer group](./default-group.md) and the [credentials](./authentication.md).

At startup the runtime connects the broker and receives a [`ConnectedRedisBroker`](api:ruststream_fred::ConnectedRedisBroker).
The connect opens the pool and waits for it, so an unreachable server fails startup instead of the first publish.
Every subscription and every publisher of the service opens against that connected form.
At [shutdown](./shutdown.md) the runtime turns it into a [`ClosedRedisBroker`](api:ruststream_fred::ClosedRedisBroker).

Every connection the broker opens reconnects after the server drops it, from 100 ms up to 10 s apart with no limit on the attempts; a Pub/Sub subscription subscribes again, and a consumer group the server lost is created again.

## Example

The service reads the stream `orders` in the group `workers`.
`cargo run --example redis_broker --features testing -- run` starts it against a server on `localhost:6379`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_broker.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_broker.rs#app"
```

</TabItem>
</Tabs>
