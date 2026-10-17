---
sidebar_position: 4
title: partition_key
description: "partition_key keys one message, so workers(n, by_key) on the consuming side keeps the order of messages sharing that key."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# partition_key

[`partition_key`](api:ruststream_fred::RedisPublishSteps::partition_key) keys one message, so `workers(n, by_key)` on the consuming side keeps the order of messages sharing that key.

`workers(n, by_key)` runs a subscription on `n` lanes, and deliveries with one partition key always share a lane.
The key travels in the [`PARTITION_KEY_HEADER`](api:ruststream_fred::PARTITION_KEY_HEADER) header, which is where the consuming side reads it.
Writing that header at the call site works the same, and carries across brokers.

The step goes anywhere after `.message(..)` on the publish builder, so it composes with a header contract the message type declares.
It writes through [`RedisPublishOptions`](api:ruststream_fred::RedisPublishOptions), the per-message options of every publisher in this crate.
The slot is bounded as `Out<impl Publisher<Options = RedisPublishOptions>, Slot>`, which keeps the step off another broker's publisher.
Naming that type is the one case where a handler body imports this crate's prelude instead of the framework's alone.
A publish without the step carries no options and leaves the headers as they are.

## Example

The intake forwards each order keyed by its customer; the ledger books on four lanes, and one customer's orders keep their order.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_publishing_partition_key.rs#partition_key"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_publishing_partition_key.rs#partition_key"
```

</TabItem>
</Tabs>
