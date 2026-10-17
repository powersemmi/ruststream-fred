---
sidebar_position: 1
title: Cluster
description: "RedisBroker::cluster takes seed nodes, of which one reachable node is enough; the client discovers the rest of the cluster."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Cluster

[`RedisBroker::cluster`](api:ruststream_fred::RedisBroker::cluster) takes seed nodes, of which one reachable node is enough; the client discovers the rest of the cluster.

Each seed is a `host:port` address, and a seed without a port means `6379`.
At startup the client reads the slot map from the first seed that answers.
From then on it sends each command to the node that owns its key's hash slot.
A cluster seed list carries no password, so a cluster that requires one takes [`credentials`](./authentication.md).

Redis refuses a command or a transaction whose keys live in different hash slots.
So a [transaction](../transactions/index.md) on a cluster refuses to begin.
A [reliable list](../lists/reliable.md) moves an entry between its queue and its processing list in one command, so both keys need one slot.
A hash tag gives them one: in `{jobs}` and `{jobs}.processing` only `jobs` is hashed.
A reliable list whose two keys hash apart refuses to start, and the error names the hash tag that fixes it.
In `#[outgoing(name = "..")]` braces mark a name template, so a type published to a tagged key declares no name, and the call site names the key.

A [`TestApp`](../testing/index.md) test runs a cluster service in process.
The in-process Redis keeps the cluster topology and refuses what spans hash slots, as a cluster does.

## Example

The service works a reliable list under the hash tag `{jobs}` and reaches the cluster through two seeds.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_broker_cluster.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_broker_cluster.rs#app"
```

</TabItem>
</Tabs>
