---
sidebar_position: 10
description: "在集群上运行 Redis 服务：种子节点、用于协同工作的键的哈希标签、分片 Pub/Sub，以及拒绝跨槽命令的测试。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Redis 集群

本指南在 Redis 集群上运行服务，并为键命名，让一条命令一起使用的键共享一个哈希槽。

示例是一个商店后端。
它从工作队列执行任务，从流开具订单发票，并在分片频道上监听事件。

## 连接 {/* #connecting */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/cluster.rs#app"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/cluster.rs#app"
```

</TabItem>
</Tabs>

[`RedisBroker::cluster`](../components/broker/cluster.md) 接收种子节点。
一个可达的种子节点就够了：客户端从它读取集群的布局，并连接到每个主节点。
与 Broker 的每个构造函数一样，它不执行任何 I/O；连接在服务启动时才打开。

在单机服务器和集群之间，处理器不需要改动。
改变的是服务为键命名的方式。

## 哈希标签 {/* #hash-tags */}

集群把键分布到 16384 个哈希槽上，每个主节点拥有其中的一段。
涉及多个键的命令、`MULTI` 块或脚本，只有在这些键都位于同一个槽时才能运行。
哈希标签决定槽：当键中有一部分在花括号内时，只对这部分计算哈希。
`{jobs}` 和 `{jobs}.processing` 共享一个槽，`{orders}` 和 `{orders}:invoices` 也一样。

花括号中的名字不能作为类型的固定去向，因为 `#[outgoing(name = "..")]` 会把花括号当作名字模板。
生产者在发布处指定这样的键：`.message(&job).to("{jobs}")`。

## 工作队列 {/* #a-work-queue */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/cluster.rs#jobs"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/cluster.rs#jobs"
```

</TabItem>
</Tabs>

[可靠列表](../components/lists/reliable.md)用一条 `LMOVE` 把每个条目移到处理中列表，所以两个键必须位于同一个槽。
处理中列表是队列的名字后接 `.processing`，它保留了队列的哈希标签。
两个键落在不同槽的可靠列表无法启动，错误会指出应当使用的哈希标签。

## 流所在槽上的副作用 {/* #side-effects-on-the-streams-slot */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/cluster.rs#orders"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/cluster.rs#orders"
```

</TabItem>
</Tabs>

流是一个键，所以流订阅在任何名字下都能工作。
[原子窗口](pipelining.md#atomic-segments)把处理器的命令和 `XACK` 放在一个 `MULTI` 块中，这个块在流所在的槽上运行。
`invoice` 写入 `{orders}:invoices`，它位于流 `{orders}` 所在的槽上。
如果一条命令针对的键位于另一个槽，Redis 会拒绝整个块：什么也不写入，条目保持待处理。

流发布者的[事务](transactions.md)也是一个 `MULTI` 块，而它的去向并不限定在一个槽上。
因此，集群拒绝这两种事务：打开任何一种都会返回错误。

## 分片 Pub/Sub {/* #sharded-pubsub */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/cluster.rs#events"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/cluster.rs#events"
```

</TabItem>
</Tabs>

集群上的经典 Pub/Sub 把每条消息发往每个节点。
[`PubSubMode::Sharded`](../components/pubsub/pubsub-mode.md) 用 `SPUBLISH` 和 `SSUBSCRIBE` 只把消息发往拥有该频道槽的节点，所以流量随集群一起扩展。
它需要 Redis 7 或更高版本。

两种方式互不相通：分片消息永远到达不了经典订阅者，反之亦然。
频道的发布者使用与订阅相同的方式。
[模式订阅](../components/pubsub/pattern.md)只支持经典 Pub/Sub。

## 测试集群服务 {/* #testing-a-cluster-service */}

测试用 `TestApp::start` 启动生产环境的应用，使用集群 Broker。
[进程内 Redis](../components/testing/index.md) 的行为与集群一致：它拒绝跨越哈希槽的命令或块。
因此，测试检查每种形式都能在服务的哈希标签下运行，没有哈希标签的可靠列表无法启动，跨两个槽的块什么也不提交。
当 `REDIS_CLUSTER_TEST_URL` 指向一个节点时，同样的测试主体会在真实集群上运行。

故障转移和重新分片只发生在真实集群上；[生产环境指南](production.md)讲解 Broker 届时的行为。
