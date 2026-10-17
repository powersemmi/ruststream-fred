---
sidebar_position: 3
title: from_pool
description: "from_pool wraps a fred Pool the service built itself, for client settings the broker's own builders do not reach."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# from_pool

[`RedisBroker::from_pool`](api:ruststream_fred::RedisBroker::from_pool) wraps a `fred` `Pool` the service built itself, for client settings the broker's own builders do not reach.

`fred`'s `Builder` sets what the broker leaves at `fred`'s defaults: a command timeout, a reconnection policy, buffer sizes, any topology a `fred` `Config` describes.
The service builds the pool in its `#[ruststream::app]` builder and hands it over unconnected.
The broker dials it when the service starts, and an unreachable server fails the start, as it does for a broker built from an address.
A pool the service has already connected is adopted as it is.
The pool's size is the [pool](./pool.md) of the broker.
Each subscription's own connection is built from the pool's config and reconnects by the pool's policy.

The [generated document](../asyncapi/server.md) reports no host for such a broker, because the address sits inside the pool's own config.
A [`TestApp`](../testing/index.md) test runs the same app in process: it reads the pool's size and config and dials nothing.

## Example

The dependency line:

```toml
fred = "10"
```

The pool holds eight connections, bounds every command at two seconds and reconnects every half second.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_broker_from_pool.rs#pool"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_broker_from_pool.rs#pool"
```

</TabItem>
</Tabs>
