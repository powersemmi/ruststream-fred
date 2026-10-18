---
sidebar_position: 1
title: reliable
description: "reliable moves each entry to a processing list and removes it on ack, which makes the queue at-least-once."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# reliable

[`reliable`](api:ruststream_fred::list::RedisList::reliable) moves each entry to a processing list and removes it on ack, which makes the queue at-least-once.

The subscription claims an entry with `BLMOVE`, which moves it from the queue to the [processing list](./processing.md) in one command.
The entry stays there while the handler runs.
An ack removes it with `LREM`.
A [`retry`](api:ruststream::runtime::HandlerOutcome::retry) pushes it back onto the queue with `LPUSH`, behind the entries already waiting, and then removes it from the processing list.
A crash between those two commands leaves the entry in both lists, so it is handled twice rather than lost.
A [`drop`](api:ruststream::runtime::HandlerOutcome::drop) removes it, as an ack does.
A consumer that dies after the move leaves its entry on the processing list; a [recovery set](./recovery.md) returns such entries to the queue.

## Example

The render farm turns the first job away while it warms up, so that job goes back on the queue and is rendered when it is popped again.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_lists.rs#reliable"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_lists.rs#reliable"
```

</TabItem>
</Tabs>
