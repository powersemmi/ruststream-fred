---
sidebar_position: 5
title: default_group
description: "default_group names the consumer group a bare #[subscriber(\"key\")] reads through; without it such a subscription refuses to start."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# default_group

[`default_group`](api:ruststream_fred::RedisBroker::default_group) names the consumer group a bare `#[subscriber("key")]` reads through; without it such a subscription refuses to start.

A bare name subscribes to the stream under that key and reads its fresh entries.
Redis reads a stream only through a consumer group, and a bare name names none.
The broker's default group supplies it, for every bare name of the broker.
A subscription created at startup creates its group too, so the stream and the group need not exist beforehand.

Without a default group the service does not start.
The error is [`RedisError::InvalidOptions`](api:ruststream_fred::RedisError::InvalidOptions), and its message names the key and the setting.
A [`RedisStream`](../streams/index.md) descriptor names its group itself, and the default group does not apply to it.

## Example

The handler reads the stream `orders` by its bare name, in the group `workers`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_broker_default_group.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_broker_default_group.rs#app"
```

</TabItem>
</Tabs>
