---
sidebar_position: 3
title: RedisListPublish
description: "RedisListPublish, Publish in the list prelude, pushes with LPUSH and can re-arm a key expiry at every publish."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# RedisListPublish

[`RedisListPublish`](api:ruststream_fred::list::RedisListPublish), `Publish` in the list prelude, pushes with `LPUSH` and can re-arm a key expiry at every publish.

The message's name is the list key.
A [list consumer](../lists/index.md) pops from the other end, so the list is a first-in, first-out queue.
Headers travel beside the payload in the broker's [envelope](../subscriptions/envelopes.md); [`envelope`](api:ruststream_fred::list::RedisListPublish::envelope) names another, which the consumer reads too.

[`ttl`](api:ruststream_fred::list::RedisListPublish::ttl) sets a time to live on the list key and arms it again with `PEXPIRE` at every publish.
A queue in use never expires; a queue nobody pushes to for the whole interval does, with whatever it still holds.
The expiry is the key's: Redis lists expire as a whole, never per element.
The push and the `PEXPIRE` leave in one round trip but are two commands, not one transaction.

The policy pairs into a [`RedisListPublisher`](api:ruststream_fred::list::RedisListPublisher) at startup.

## Example

Each order asks for a shipping label on the `label-jobs` list, whose key expires after an hour without a push.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_publishing_list.rs#list_publish"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_publishing_list.rs#list_publish"
```

</TabItem>
</Tabs>
