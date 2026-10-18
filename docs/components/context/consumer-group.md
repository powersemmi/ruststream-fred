---
sidebar_position: 1
title: ConsumerGroup
description: "Ctx<keys::ConsumerGroup> binds the consumer group a stream subscription reads through, on a single delivery or a batch."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# ConsumerGroup

[`Ctx<keys::ConsumerGroup>`](api:ruststream_fred::context::keys::ConsumerGroup) binds the consumer group a stream subscription reads through, on a single delivery or a batch.

The group is fixed when the subscription opens: the descriptor's `.group(..)`, or the broker's `default_group` for a subscription that names none.
A handler that reads it learns the group at run time, so one body serves whichever group it is mounted under.
As a `Ctx` parameter the key yields an owned `String`.
Read off a context with `ctx.context(keys::ConsumerGroup)`, it yields a `&str` borrowed from the context.
A batch body reads it that way off [`StreamBatchContext`](./batch-context.md), since the group belongs to the whole subscription.

## Example

The subscription names only the stream; the group comes from the broker's default, and each invoice records it.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_context_consumer_group.rs#consumer_group"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_context_consumer_group.rs#consumer_group"
```

</TabItem>
</Tabs>
