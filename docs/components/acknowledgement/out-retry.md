---
sidebar_position: 5
title: out_retry
description: "Each descriptor names where a retry copy goes; a pattern names none, so its registration names the channel with out_retry."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# out_retry

Each descriptor names where a retry copy goes; a pattern names none, so its registration names the channel with [`out_retry`](api:ruststream::runtime::Mounting::out_retry).

The runtime publishes a copy for a [`retry_after`](./retry-after.md), for a retry under a [cap](./dead-letter.md), and for a dead letter.
A stream, a list and a channel subscription each answer with their own key or channel, so the copy reaches the same subscription again.
A copy leaves through [`RedisDefaultPublish`](api:ruststream_fred::RedisDefaultPublish) unless the registration names another policy.
That publisher writes a name the way the service reads it: `XADD` to a stream, `LPUSH` to a list in its framing, `PUBLISH` to a channel in its mode and framing.
`.out_retry(policy)` replaces the policy, and `.transform(..)` after it changes each copy on its way out.
A [pattern](../pubsub/pattern.md) reads many channels and names none a `PUBLISH` can reach.
Its registration names the channel with `.out_retry(policy).to(channel)`, or mounts a publish transform that declares `Destination = Names` and names one per delivery.
A pattern registration that names neither does not start, and the error names the subscription and both fixes.

## Example

The fraud screen reads every `payments.*` channel; a retry copy goes to `payments.retry`, which the pattern reads again.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_acknowledgement_out_retry.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_acknowledgement_out_retry.rs#app"
```

</TabItem>
</Tabs>
