---
sidebar_position: 2
title: Channels
description: "A stream channel reports its group, consumer, read mode and idle threshold; a list its acknowledgement; a channel its mode."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Channels

A stream channel reports its group, consumer, read mode and idle threshold; a list its acknowledgement; a channel its mode.

Each channel a service reads carries `form`, the Redis structure behind it: `stream`, `list` or `pubsub`.
The rest of the extension is the descriptor's own settings.

| Form | Fields |
|---|---|
| `stream` | `group`, `consumer`, `readMode` (`fresh`, `reclaim` or `claiming`), and `minIdleMs` for the two modes that claim |
| `list` | `reliable`, `processing` (the processing list of a reliable list), `envelope` |
| `pubsub` | `mode` (`classic` or `sharded`), `pattern`, `envelope` |

A [`RedisPubSubPattern`](../pubsub/pattern.md) channel reports `pattern: true`, so a reader knows the address is a glob and not a channel to publish to.
`envelope.contentType` is the framing a list entry or a channel message carries its headers in.
A stream entry holds its headers as fields, so a stream has no envelope.
A setting the descriptor leaves to the broker, such as a consumer name it did not set, stays out.

## Example

The channel `orders` reports a claiming read in the group `workers`, as the consumer `worker-1`, from thirty seconds of idle time.
The channel `jobs` reports a reliable list, and `events` a sharded channel.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_asyncapi.rs#channels"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_asyncapi.rs#channels"
```

</TabItem>
</Tabs>
