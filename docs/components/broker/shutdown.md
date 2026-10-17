---
sidebar_position: 10
title: Shutdown
description: "Shutdown unblocks every read and closes every connection; a publisher used afterwards gets RedisError::ShutDown at once."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Shutdown

Shutdown unblocks every read and closes every connection; a publisher used afterwards gets [`RedisError::ShutDown`](api:ruststream_fred::RedisError::ShutDown) at once.

When the service stops, the runtime runs the `on_shutdown` hooks while the broker is still connected and then shuts the broker down.
A stream or list subscription may be waiting in a blocking read on its own connection.
The broker sends `CLIENT UNBLOCK` for each such read, so the read ends at once instead of at its timeout, and closes the connection.
Then the pool sends `QUIT` on every connection, and the `after_shutdown` hooks run.

A publisher taken before the shutdown keeps pointing at the closed connection.
A publish through it, an acknowledgement of a delivery or a transaction returns `RedisError::ShutDown`, and never runs against the dead pool.
A tool that drives the broker by hand receives a [`ClosedRedisBroker`](api:ruststream_fred::ClosedRedisBroker) from the shutdown; its [`connections_closed`](api:ruststream_fred::ClosedRedisBroker::connections_closed) is the number of pooled connections it closed.

## Example

The service answers each order with a shipment, and logs once its connections are closed.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_broker_shutdown.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_broker_shutdown.rs#app"
```

</TabItem>
</Tabs>
