---
sidebar_position: 1
title: RedisDefaultPublish
description: "A reply with no named policy writes each name the way this service reads it, and XADDs a name it does not read."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# RedisDefaultPublish

A reply with no named policy goes through [`RedisDefaultPublish`](api:ruststream_fred::RedisDefaultPublish), which writes each name the way this service reads it, and `XADD`s a name it does not read.

The runtime wires it for every registration that replies and names no policy: `b.include(handler)` with no `.out_reply(..)`.
Its live form, [`RedisDefaultPublisher`](api:ruststream_fred::RedisDefaultPublisher), looks up how the service reads the destination.
A name the service reads as a list gets an `LPUSH`, a channel a `PUBLISH`, a stream an `XADD`, each in the [framing](../subscriptions/framing.md) that subscription decodes.
A name the service does not read gets an `XADD`.
The choice is made per name, so one handler's replies may go to a list and another's to a stream.
A mount site that names a policy, such as [`RedisPublish`](./index.md), overrides the default.

## Example

The planner replies to `jobs` without naming a policy; the service reads `jobs` as a reliable list, so each job is pushed onto it.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_publishing_default.rs#default"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_publishing_default.rs#default"
```

</TabItem>
</Tabs>
