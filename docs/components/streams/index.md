---
title: RedisStream
description: "RedisStream::new reads fresh entries off a stream through a consumer group, and each ack is an XACK."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# RedisStream

[`RedisStream::new`](api:ruststream_fred::stream::RedisStream::new) reads fresh entries off a stream through a consumer group, and each ack is an `XACK`.

The read is `XREADGROUP` with the id `>`: entries no consumer of the group has been handed yet.
[`group`](api:ruststream_fred::stream::RedisStream::group) names the group, and a descriptor without one stops the service at startup.
The subscription creates its group when it opens, with `XGROUP CREATE ... MKSTREAM`, so neither the group nor the stream has to exist first.
Where a new group starts is its [`StreamStart`](./stream-start.md).
Every group reads every entry of the stream.
The consumers of one group share its entries: each entry goes to one of them.
[`consumer`](api:ruststream_fred::stream::RedisStream::consumer) names this subscription's consumer; without it each subscription gets a unique generated name.
An entry is handed out once and waits in the group's pending entries list until its handler settles it.
An ack is `XACK`, which takes it off that list.
An entry whose consumer died before settling stays there, and a [`reclaim`](./reclaim.md) or a [`claiming`](./claiming.md) subscription takes it back.
The three constructors are the three read modes, because each returns a different set of entries.
A stream keeps its entries until it is trimmed, so a group can be [moved](../positions/index.md) back over them or forward past them.

## Example

Billing and shipping read every order, each in its own group.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_streams_new.rs#groups"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_streams_new.rs#groups"
```

</TabItem>
</Tabs>
