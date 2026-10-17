---
sidebar_position: 6
description: "用 Redis Pub/Sub 向每个运行中的服务广播消息：频道、模式、慢订阅者、分片频道，以及会丢失什么。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 用 Pub/Sub 广播

本指南用 Redis Pub/Sub 把消息广播给服务的每个运行实例。

示例是一个网店前台。
价格变动时，每个实例都刷新自己的价格缓存；它还通过模式跟踪订单事件，并宣布已发货的订单。

## 每个订阅者都收到消息 {/* #every-subscriber-receives-the-message */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/fan_out.rs#broadcast"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/fan_out.rs#broadcast"
```

</TabItem>
</Tabs>

[`RedisPubSub`](../components/pubsub/index.md) 订阅一个频道。
发布到频道的消息到达此刻已连接的每个订阅：`refresh_price` 和 `reindex_price` 都会收到它，在每个运行实例中都是如此。
消息不在订阅之间分摊，也不需要确认；处理器返回的结果不结算任何东西。

Redis 不保存副本。
没有订阅连接时发布的消息就此丢失，发布给正在重启的实例的消息也一样。
只有 Redis 把频道路由到订阅之后，订阅才会打开，所以服务启动之后发布的消息不会丢失。

## 慢订阅者 {/* #a-slow-subscriber */}

消息一发布，Redis 就推送它，不管处理器是否就绪。
订阅暂存处理器还没取走的消息，上限是它的[缓冲区](../components/pubsub/buffer.md)：除非 `buffer` 设置了别的大小，否则是 32 条消息。
缓冲区满了以后，每条新消息都会挤掉最旧的一条，订阅会记录一条警告，写明频道和丢失的数量。
`refresh_price` 把缓冲区设为一千条消息，所以一阵密集的价格变动会排队等它处理，而不会被截断。
缓冲区在订阅打开时预留，每条消息约 600 字节，与载荷大小无关。

## 模式 {/* #patterns */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/fan_out.rs#pattern"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/fan_out.rs#pattern"
```

</TabItem>
</Tabs>

[`RedisPubSubPattern`](../components/pubsub/pattern.md) 用 `PSUBSCRIBE` 订阅一个通配模式，这里是 `orders.events.` 下的每个频道。
处理器通过 `Ctx<keys::Channel>` 从 [Pub/Sub 上下文](../components/context/pubsub-context.md)读取消息到达的频道。
`keys::FromPattern` 表示消息是否通过模式匹配而来。

## 发布到频道 {/* #publishing-to-a-channel */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/fan_out.rs#announce"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/fan_out.rs#announce"
```

</TabItem>
</Tabs>

`announce_shipment` 从流中读取发货记录，并在频道 `orders.events.shipped` 上以一个 `OrderEvent` 作为回复。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/fan_out.rs#app"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/fan_out.rs#app"
```

</TabItem>
</Tabs>

回复经由 [`pubsub::Publish`](../components/publishing/pubsub-publish.md) 发出，它发送 `PUBLISH`。
对于本服务没有任何订阅读取的名字，Broker 的默认发布者会把它当作流的键，并向它发送 `XADD`。
模式订阅并不按名字读取 `orders.events.shipped`，所以注册要指定 Pub/Sub 策略。
同一个服务中的模式订阅会收到这条通知。

模式没有指定可以发布副本的频道。
因此，`track_order` 的注册用 `out_retry(..).to("orders.events.retry")` 指定一个频道，没有它服务无法启动。
这个频道匹配该模式，所以重试的事件会回到处理器，在每个运行实例中都是如此。

## 集群上的分片频道 {/* #sharded-channels-on-a-cluster */}

在[集群](../components/broker/cluster.md)上，经典的 `PUBLISH` 发往每个节点。
[`PubSubMode::Sharded`](../components/pubsub/pubsub-mode.md) 改用 `SSUBSCRIBE` 和 `SPUBLISH`：频道位于拥有其哈希槽的节点上，所以流量分散到整个集群。
它需要 Redis 7 或更高版本，也不支持模式订阅。
两种方式互不相通，所以订阅和发布策略要指定同一种方式：`RedisPubSub::new(..).mode(PubSubMode::Sharded)` 和 `pubsub::Publish::new().mode(PubSubMode::Sharded)`。

## 连接断开时 {/* #when-the-connection-drops */}

连接断开、Redis 重启或故障转移之后，Broker 会重新连接，每个 Pub/Sub 订阅都会重新订阅。
断开期间发布的消息不会投递。
重新分片把分片订阅的槽移到另一个节点之后，这个订阅不会恢复。

## 何时改用流 {/* #when-to-use-a-stream-instead */}

Pub/Sub 适合只在当下有用的内容：要刷新的缓存、实时仪表盘、发给每个实例的通知。
如果每个读者都必须收到每条消息，包括它停机期间发布的消息，就使用流，并为每个读者配一个[消费者组](consumer-groups.md)。
每个组读取每个条目，条目一直留在流中，直到被裁剪。

示例文件中的测试发布一次价格变动和一次发货，并检查每个订阅都收到了发布的内容。
