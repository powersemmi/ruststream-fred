---
title: pipeline
description: ".pipeline() settles a subscription in a window, and a handler queues its own Redis commands to leave with its acknowledgement."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# pipeline

[`.pipeline()`](api:ruststream_fred::pipeline::RedisPipelineSteps::pipeline) settles a subscription in a window, and a handler queues its own Redis commands to leave with its acknowledgement.

The step chains where the handler is mounted, `b.include(handler.pipeline())`, like `.workers(..)` or [`.block(..)`](../subscriptions/block.md).
It applies to a [`RedisStream`](../streams/index.md), a [`RedisList`](../lists/index.md), a [`RedisPubSub`](../pubsub/index.md) and a [`RedisPubSubPattern`](../pubsub/pattern.md), and keeps every setting the descriptor carries.
Each form's [prelude](../broker/prelude.md) brings the step in with [`RedisPipelineSteps`](api:ruststream_fred::pipeline::RedisPipelineSteps).

A handler takes the delivery's round with `Ctx<keys::Pipeline>`, a [`RedisPipeline`](api:ruststream_fred::pipeline::RedisPipeline).
What it queues there runs after the handler returns, in one pipeline with the settles of the whole window: under load, one round trip per fetched batch instead of one per command.
The queued commands follow the delivery's outcome: they leave when the delivery is [acknowledged](../acknowledgement/index.md) and are dropped otherwise.
So a handler's Redis side effects never land for a delivery that comes back.
The [Window](../../architecture/window.md) page draws a window from the read to the flush.

A handler that reads `Ctx<keys::Pipeline>` and is mounted without `.pipeline()` does not compile.
The error is the framework's general one: the handler's context, [`PipelineContext`](api:ruststream_fred::context::PipelineContext), does not match the subscription's.
A delivery whose handler queues nothing costs the window no buffer.
[`.atomic()`](./atomic.md) after the step makes each delivery's segment a transaction.

## Example

An accepted order is recorded on a list in the same round trip as its `XACK`; an order without items is dropped, and its push with it.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_pipelining.rs#pipeline"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_pipelining.rs#pipeline"
```

</TabItem>
</Tabs>
