---
sidebar_position: 4
title: InRound
description: "The InRound transform on the reply position puts a handler's reply into its delivery's round, after the commands it queued."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# InRound

The [`InRound`](api:ruststream_fred::pipeline::InRound) transform on the reply position puts a handler's reply into its delivery's round, after the commands it queued.

The transform chains after the reply policy: `.out_reply(Publish).transform(InRound)`.
The reply then leaves with the delivery's settle, in the window's pipeline, and only when the delivery is acknowledged.
It follows what the handler queued and published through a [bound](./bind.md) slot, so a reader sees the handler's writes in the order the handler made them.
Without the transform a reply is an ordinary publish: it leaves as the handler returns, before the window sends what the handler queued.
The transform writes the crate's per-message options, so it mounts only where this crate's policies publish.

## Example

The reply confirms the order on the same log the bound slot wrote to; with `InRound` the log reads accepted, then confirmed.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_pipelining_round.rs#in_round"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_pipelining_round.rs#in_round"
```

</TabItem>
</Tabs>
