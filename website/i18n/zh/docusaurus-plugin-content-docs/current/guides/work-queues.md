---
sidebar_position: 4
description: "在 Redis 列表上运行工作队列：简单队列、带处理中列表的可靠队列、找回停止的工作者的任务，以及过期。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 基于列表的工作队列

本指南在 Redis 列表上运行工作队列，从简单队列一直到能找回停止的工作者的任务的可靠队列。

示例是一个图片服务。
它从一个队列预热页面缓存，从另一个队列渲染缩略图。

## 简单队列 {/* #a-simple-queue */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/work_queues.rs#simple"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/work_queues.rs#simple"
```

</TabItem>
</Tabs>

生产者用 `LPUSH` 把条目推入列表的左端。
[`RedisList::new`](../components/lists/index.md) 用 `BRPOP` 从右端弹出，所以条目按进入的顺序出来，每个条目恰好到达一个消费者。
服务的每个运行实例都从同一个列表弹出，各实例分摊工作。

弹出操作在处理器运行之前就从 Redis 中删除了条目。
如果服务在处理器运行时停止，条目就没了。
这个队列对每个条目至多投递一次，处理器的结果不结算任何东西。
错过一次缓存预热，代价只是一次较慢的页面加载，所以 `cache.warmups` 是一个简单队列。

## 可靠队列 {/* #a-reliable-queue */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/work_queues.rs#reliable"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/work_queues.rs#reliable"
```

</TabItem>
</Tabs>

[`reliable`](../components/lists/reliable.md) 不弹出条目，而是用一条命令 `BLMOVE` 把每个条目移到处理中列表。
处理中列表是 `{thumbnails}.processing`，即队列的键后接 `.processing`，除非 [`processing`](../components/lists/processing.md) 指定了别的名字。
处理器运行期间，条目一直留在那里：

- `ack` 用 `LREM` 把它从处理中列表删除；
- `retry` 把它移回队列；
- `drop` 删除它。

因此，只有处理器处理完任务之后，任务才离开 Redis，这个队列对每个条目至少投递一次。

## 找回停止的工作者的任务 {/* #recovering-a-stopped-workers-job */}

在移动和确认之间停止的工作者，会把它的条目留在处理中列表上。
列表不记录谁在什么时候取走了条目，所以没有任何东西会自动把它送回。

[`recovery_zset`](../components/lists/recovery.md) 为此启用一个看门狗。
每次移动都会把条目记入一个有序集合，分值是取走它的时间，确认会删除这条记录。
订阅在读取的同时，把空闲超过 `min_idle` 的每个条目移回队列，任何工作者都可以取走它。
使用恢复集合时必须设置 `min_idle`，没有它的订阅无法启动。
把它设得比任务最长的耗时还长：值太短，会从仍在处理的工作者手中拿走任务，缩略图就会被渲染两次。
[`recovery_ttl`](api:ruststream_fred::list::RedisList::recovery_ttl) 让被放弃的恢复集合过期，它必须比 `min_idle` 长。

分值是挂钟时间的毫秒数，所以服务各主机的时钟需要一致。

## 向队列投放任务 {/* #feeding-the-queue */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/work_queues.rs#producer"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/work_queues.rs#producer"
```

</TabItem>
</Tabs>

`queue_thumbnail` 从流中读取上传，并用一个缩略图任务回复每次上传。
任务发往 `{thumbnails}`，由挂载点指定。
`#[outgoing(name = ..)]` 中的名字会把花括号当作名字模板，所以带哈希标签的键改在挂载点上指定。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/work_queues.rs#app"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/work_queues.rs#app"
```

</TabItem>
</Tabs>

回复经由 [`list::Publish`](../components/publishing/list-publish.md) 发出，它用 `LPUSH` 推入。
它的 [`ttl`](api:ruststream_fred::list::RedisListPublish::ttl) 在每次推入时重新设置队列键的过期时间。
正在使用的队列永不过期，一天没人投放任务的队列会连同其中剩余的内容一起过期。
列表没有单个条目的过期时间，所以过期总是作用于整个队列。

## 集群上的队列 {/* #queues-on-a-cluster */}

在[集群](../components/broker/cluster.md)上，可靠队列和它的处理中列表必须位于同一个哈希槽，因为一条命令要在两者之间移动条目。
哈希标签让它们落在同一个槽：Redis 只对键中花括号内的部分计算哈希，所以 `{thumbnails}` 和 `{thumbnails}.processing` 落在同一个节点上。
两个键落在不同槽的可靠队列无法启动，错误会指出应当使用的哈希标签。
示例的键已经带有标签，所以它不加改动就能迁到集群上。

## 何时改用流 {/* #when-to-use-a-stream-instead */}

列表不保留历史：处理过的条目就没了，第二个服务也无法再读到它。
恢复看门狗是列表唯一的重新投递方式，列表本身也没有投递次数。
如果多个服务读取同一份工作，工作需要重放，或者停止的工作者的任务必须在没有看门狗的情况下回来，就使用带消费者组的流，如[消费者组与崩溃恢复](consumer-groups.md)所示。

示例文件中的测试发布一次预热和一次上传，并检查处理器收到了什么。
其中一个测试把时钟推进到超过 `min_idle`，检查已经渲染的任务不会回来。
