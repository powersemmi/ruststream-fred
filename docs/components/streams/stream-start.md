---
sidebar_position: 3
title: StreamStart
description: "StreamStart says where a consumer group starts when the subscription creates it: the beginning, the end, or after an id."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# StreamStart

[`StreamStart`](api:ruststream_fred::stream::StreamStart) says where a consumer group starts when the subscription creates it: the beginning, the end, or after an id.

A descriptor names it with [`start_id`](api:ruststream_fred::stream::RedisStream::start_id).
`StreamStart::New` is the default: the group reads only entries added after it was created.
`StreamStart::Beginning` makes the group read every entry the stream still holds, then the new ones.
`StreamStart::Id(id)` starts the group after that entry id, which it does not deliver.
The setting is read once, when the group is created.
A group that already exists keeps its own cursor, whatever a later start names.
A group that has to move on every start is a [`start_at`](../positions/start-at.md) subscription.

## Example

A reporting group added to a running system reads the orders the stream already holds, the first time it is created.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_streams_start.rs#start"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_streams_start.rs#start"
```

</TabItem>
</Tabs>
