---
sidebar_position: 1
title: start_at
description: "start_at moves the group to a position before the first delivery, on every startup; the whole group moves, not one consumer."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# start_at

`start_at` moves the group to a [position](./index.md) before the first delivery, on every startup; the whole group moves, not one consumer.

It is a clause of the attribute, `start_at(RedisGroupPosition::beginning())`, or [`.start_at(position)`](api:ruststream::runtime::SubscriberSettings::start_at) after `build()` in the manual form.
The move happens whatever the group read and acknowledged before.
With `beginning()` the group reads every retained entry again, the acknowledged ones included.
Every consumer of the group follows the move, and a consumer blocked in a read sees it within one [`block`](../subscriptions/block.md).
It suits a read model rebuilt from the retained stream on each boot.
A [`StreamStart`](../streams/stream-start.md) is different: it applies only when the subscription creates the group.
The `block` step reads the same before and after `start_at`.

## Example

The audit projection is rebuilt from the oldest retained entry on every startup, although its group acknowledged everything it read before.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_positions_start_at.rs#start-at"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_positions_start_at.rs#start-at"
```

</TabItem>
</Tabs>
