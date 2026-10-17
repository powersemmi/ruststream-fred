---
sidebar_position: 3
title: processing
description: "processing names the list an entry waits on while handled; on a cluster both keys share one hash tag, or the subscription refuses to start."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# processing

[`processing`](api:ruststream_fred::list::RedisList::processing) names the list an entry waits on while handled; on a cluster both keys share one hash tag, or the subscription refuses to start.

The processing list is where a [reliable](./reliable.md) subscription moves an entry before its handler runs.
Its default name is the queue's key with `.processing` appended, so `jobs` waits on `jobs.processing`.
Every consumer of the queue that uses the same name shares that list.
On a cluster a claim moves the entry between the two keys in one command, and Redis refuses a command that spans hash slots.
A hash tag puts both keys on one slot: Redis hashes only the part between `{` and `}`, so `{exports}` and `{exports}.in-flight` land together.
The default name follows the tag of its queue.
A subscription whose two keys hash to different slots does not start, and its error names the tag to use.

## Example

The export queue runs on a cluster, and its processing list carries the queue's tag.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_lists_processing.rs#processing"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_lists_processing.rs#processing"
```

</TabItem>
</Tabs>
