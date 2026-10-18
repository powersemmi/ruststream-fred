---
sidebar_position: 3
title: bind
description: "pipeline.bind(out) makes a slot's publishes join the delivery's round, so they leave only if the delivery is acknowledged."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# bind

[`pipeline.bind(out)`](api:ruststream_fred::pipeline::RedisPipeline::bind) makes a slot's publishes join the delivery's round, so they leave only if the delivery is acknowledged.

The handler takes the slot as `Out<impl Bindable>`, where [`Bindable`](api:ruststream_fred::pipeline::Bindable) is the bound every publisher of this crate meets.
`bind` hands the slot back with its codec and transforms, and every publish through it from then on is queued into the round.
A bound publish leaves with the delivery's settle, after the commands the handler queued before it.
A slot publish the handler does not bind leaves at once, whatever the delivery's outcome.
The slot's policy is one of this crate's: [`stream::Publish`](../publishing/index.md), [`list::Publish`](../publishing/list-publish.md) or [`pubsub::Publish`](../publishing/pubsub-publish.md); another broker's publisher does not compile there.
A reply joins the round through the [InRound](./in-round.md) transform.

## Example

The handler writes an order's first step to the order log through a bound slot, so a dropped order leaves no entry.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_pipelining_round.rs#bind"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_pipelining_round.rs#bind"
```

</TabItem>
</Tabs>
