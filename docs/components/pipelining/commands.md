---
sidebar_position: 2
title: Commands
description: "RedisPipeline queues fred's commands, Lua through eval and evalsha, and any other command through custom; a reply is not read back."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Commands

[`RedisPipeline`](api:ruststream_fred::pipeline::RedisPipeline) queues `fred`'s commands, Lua through `eval` and `evalsha`, and any other command through `custom`; a reply is not read back.

The pipeline carries `fred`'s command methods for keys, strings, hashes, lists, sets, sorted sets, streams, functions and `PUBLISH`, under `fred`'s names and arguments.
Each method queues its command into the delivery's segment and returns `Result<(), fred::error::Error>`.
`Ok` means queued; an error is `fred`'s refusal at queue time, such as an argument that does not convert.
The command runs after the handler returns, so its reply never reaches the handler.
A command whose answer the handler needs runs on the pool instead, through [`FredPool`](../context/fred-pool.md).

[`eval`](api:ruststream_fred::pipeline::RedisPipeline::eval) queues a Lua script by its source and [`evalsha`](api:ruststream_fred::pipeline::RedisPipeline::evalsha) by its SHA1 digest, each with its keys and arguments.
On a cluster a script goes to the node of its first key.
[`custom`](api:ruststream_fred::pipeline::RedisPipeline::custom) queues any command by name, for one the methods do not cover.
The first command a delivery queues opens a `fred` pipeline for it; a delivery that queues nothing allocates none.
The in-process Redis of the [test harness](../testing/index.md) runs no scripts, so a test of a script runs against a server.

## Example

The handler queues two commands: a push onto the shipping queue and a counter.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_pipelining_commands.rs#commands"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_pipelining_commands.rs#commands"
```

</TabItem>
</Tabs>

The same two writes as one Lua script, so that no client sees one without the other:

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_pipelining_commands.rs#script"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_pipelining_commands.rs#script"
```

</TabItem>
</Tabs>
