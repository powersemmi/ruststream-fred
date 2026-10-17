---
sidebar_position: 2
title: FredPool
description: "Ctx<keys::FredPool> hands a handler the broker's fred connection pool on every form, for a command whose answer it needs now."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# FredPool

[`Ctx<keys::FredPool>`](api:ruststream_fred::context::keys::FredPool) hands a handler the broker's `fred` connection pool on every form, for a command whose answer it needs now.

The value is `fred::clients::Pool`, the pool the broker publishes through.
A command sent on it runs at once, and the handler reads its reply before it returns.
That is the difference from a [pipelined](../pipelining/index.md) command, which leaves after the handler, with the delivery's acknowledgement, and returns no reply.
The command methods come from `fred`'s interface traits, such as `fred::interfaces::KeysInterface`, which the handler's file imports.

As the only `Ctx` key of a handler, `FredPool` names [`PoolContext`](api:ruststream_fred::context::PoolContext), a context that carries the pool alone.
Beside a form's own key it reads that form's context: [`StreamContext`](./index.md), [`StreamBatchContext`](./batch-context.md), [`PubSubContext`](./pubsub-context.md) or the pipelined [`PipelineContext`](api:ruststream_fred::context::PipelineContext).
There it goes after the form's key in the signature, because the first `Ctx` key names the handler's context.
A list handler, whose delivery carries no context of its own, takes the pool through `PoolContext`.

## Example

Each order takes the next number of a shared sequence with `INCR`, and the reply carries it.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_context_fred_pool.rs#fred_pool"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_context_fred_pool.rs#fred_pool"
```

</TabItem>
</Tabs>
