---
title: Lists
description: "RedisList is a work queue: each entry goes to one consumer, and the simple form pops it with BRPOP and settles nothing."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Lists

[`RedisList`](api:ruststream_fred::list::RedisList) is a work queue: each entry goes to one consumer, and the simple form pops it with `BRPOP` and settles nothing.

[`RedisList::new("thumbnails")`](api:ruststream_fred::list::RedisList::new) reads the list `thumbnails`.
A producer pushes an entry with `LPUSH`, and a consumer pops it from the other end, so entries arrive in the order they were pushed.
Each entry reaches exactly one consumer: a list has no consumer groups, no fan-out and no replay.
Consumers of one list in several processes share its entries.
The simple form pops the entry with `BRPOP`, which removes it from Redis.
A handler that crashes after the pop loses the entry, so delivery is at most once.
The outcome settles nothing: an ack, a retry and a drop all report [`AckError::Unsupported`](api:ruststream::AckError::Unsupported), and the runtime logs a warning for each.
A list that has to keep an entry until its handler finishes uses the [reliable](./reliable.md) form.
How long one pop waits on the server is the subscription's `block` setting, five seconds by default.

## Example

Thumbnails are cheap to make again, so a lost job costs nothing.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_lists.rs#simple"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_lists.rs#simple"
```

</TabItem>
</Tabs>
