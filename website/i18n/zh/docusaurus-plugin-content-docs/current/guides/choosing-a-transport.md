---
sidebar_position: 2
description: "在 Redis 流、列表和 Pub/Sub 频道之间做出选择：各自保存什么，谁收到消息，确认做了什么。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 选择传输方式

本指南比较服务读取的三种 Redis 结构：流、列表和 Pub/Sub 频道。

示例是一个同时使用这三种结构的商店服务。
订单从流中读取并开具账单，缩略图从列表中读取并渲染，价格变动从频道到达。

## 流：持久的日志 {/* #streams-a-durable-log */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/choosing_a_transport.rs#stream"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/choosing_a_transport.rs#stream"
```

</TabItem>
</Tabs>

Redis 流保存每个条目，直到服务裁剪它。
[`RedisStream`](../components/streams/index.md) 通过消费者组读取流。
每个组都会收到每个条目一次，在组内由一个消费者处理它。
确认一个条目，就是对它执行 `XACK`。
消费者没有确认的条目留在组的待处理条目列表中，所以崩溃不会丢失任何东西。
用 [seek](../components/positions/seek.md) 可以从较早的条目重新读取流。

订单关系到钱，所以放在流上。
`bill` 用一张 `Invoice` 回复每个订单，回复写入流 `invoices`。

## 列表：工作队列 {/* #lists-a-work-queue */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/choosing_a_transport.rs#list"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/choosing_a_transport.rs#list"
```

</TabItem>
</Tabs>

Redis 列表是一个队列：生产者推入一个条目，恰好一个消费者把它弹出。
[`RedisList::new`](../components/lists/index.md) 用 `BRPOP` 弹出条目，所以条目在处理器运行之前就离开了 Redis，处理器中的崩溃会丢失它。
[`reliable`](../components/lists/reliable.md) 则把条目移到处理中列表，等处理器确认之后才删除它。
列表没有组，也不能重放。

渲染缩略图是交给一个工作者的工作，所以这些任务放在可靠列表上。
`render` 在频道 `thumbnails.ready` 上宣布每张完成的缩略图。

## Pub/Sub：广播 {/* #pubsub-a-broadcast */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/choosing_a_transport.rs#pubsub"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/choosing_a_transport.rs#pubsub"
```

</TabItem>
</Tabs>

Pub/Sub 消息到达发布那一刻已连接的每个订阅者。
Redis 不保存任何东西：未连接的订阅者永远看不到这条消息。
[`RedisPubSub`](../components/pubsub/index.md) 没有确认，所以处理器返回的结果不结算任何东西。

价格变动只对此刻正在运行的实例有意义，它们会刷新自己的缓存，所以价格变动放在频道上。

## 对比 {/* #side-by-side */}

| | 流 | 列表 | Pub/Sub |
|---|---|---|---|
| 无人读取时是否保留 | 保留到被裁剪 | 保留到被弹出 | 否 |
| 谁收到消息 | 每个组中的一个消费者 | 一个消费者 | 每个已连接的订阅者 |
| `ack` | `XACK` | 可靠列表：从处理中列表 `LREM`；简单列表：什么也不做 | 什么也不做 |
| `retry` | 向流追加一份副本 | 可靠列表：放回列表；简单列表：丢失 | 丢失 |
| `retry_after` | 延迟之后的一份副本 | 延迟之后的一份副本 | 延迟之后的一份副本 |
| 一批 | 一次读取最多 `n` 个条目 | 由客户端组装 | 由客户端组装 |
| 重放 | 对组执行 seek | 无 | 无 |

[确认](../components/acknowledgement/index.md)各页完整说明每种结果，[重试、延迟与死信](retries.md)展示延迟和上限在每种结构上如何工作。
流上的一[批](../components/subscriptions/batches.md)就是一次读取的 `COUNT`。
列表和频道一次投递一个条目，所以客户端收集同样大小的一批。

一个名字只对应一种结构。
流和列表是特定类型的键，写入另一种类型会返回 `WRONGTYPE` 错误。
频道根本不是键：在频道名下执行 `XADD` 会写入一个流，频道的订阅者永远看不到它。

## 一个服务，三种传输方式 {/* #one-service-three-transports */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/choosing_a_transport.rs#app"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/choosing_a_transport.rs#app"
```

</TabItem>
</Tabs>

只用一种传输方式的服务导入该传输方式的 prelude，例如 `ruststream_fred::stream::prelude`。
这个服务混用多种传输方式，所以导入 [`ruststream_fred::prelude`](../components/broker/prelude.md)。
在那里，每种传输方式的发布策略保留其模块名：`stream::Publish` 用 `XADD` 写入，`pubsub::Publish` 用 `PUBLISH` 写入。
回复类型指定回复发往哪里，挂载点上的策略指定它如何到达那里。

三个处理器的形态相同。
把处理器换到另一种传输方式，改动的是它属性中的描述符和挂载点上的策略，而不是它的主体。

示例文件中的测试向每种传输方式发布消息，并读取处理器发回的内容，全部在进程内完成。
