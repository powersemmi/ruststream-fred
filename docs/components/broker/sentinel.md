---
sidebar_position: 2
title: Sentinel
description: "RedisBroker::sentinel names the monitored primary and the sentinels that watch it; the client finds the primary through them."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Sentinel

[`RedisBroker::sentinel`](api:ruststream_fred::RedisBroker::sentinel) names the monitored primary and the sentinels that watch it; the client finds the primary through them.

The first argument is the name the sentinels monitor the primary under, such as `mymaster`.
The second is the sentinels, each a `host:port` address; a sentinel without a port means `26379`.
At startup the client asks the sentinels where the primary is and connects to it.
Every read and every write goes to the primary.

After a failover the connections reconnect, and the client asks the sentinels again, so the service follows the new primary.
A sentinel list carries no password.
The primary's password goes in [`credentials`](./authentication.md), and the sentinels' own in [`sentinel_credentials`](./sentinel-credentials.md).

## Example

The service reaches the primary `mymaster` through either of two sentinels.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_broker_sentinel.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_broker_sentinel.rs#app"
```

</TabItem>
</Tabs>
