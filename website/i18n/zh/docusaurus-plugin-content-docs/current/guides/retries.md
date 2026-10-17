---
sidebar_position: 5
description: "立即或延迟重试一条 Redis 消息，让延迟在重启后依然有效，限定尝试次数，并把一直失败的消息转入死信流。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 重试、延迟与死信

本指南立即或稍后重试一条消息，限定尝试次数，并把一直失败的消息转入[死信](../components/acknowledgement/dead-letter.md)流。

Redis 没有单条消息的延迟，没有死信结构，投递次数也只存在于流的消费者组内部。
其余部分由框架提供：挂载点声明上限和去向，订阅选择如何保存延迟。

示例是一个退款服务，它通过发卡处理商发出退款。

## 订阅者 {/* #the-subscriber */}

处理商有三种拒绝退款的方式，每种需要不同的结果。
`Busy` 表示它的速率限额已经用完：立即重试仍会被拒绝，所以 [`retry_after`](../components/acknowledgement/retry-after.md) 等待 30 秒。
`Unavailable` 表示它没有应答：`retry` 立即再试一次。
`Declined` 表示卡账户已经关闭，再试一次也是同样的答复，所以这笔退款被丢弃。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/retries.rs#refund"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#refund"
```

</TabItem>
</Tabs>

流上的 `retry` 向流追加一份条目的副本，并确认原条目。
在其他结构上，它做该结构允许的事：

| 订阅 | 没有上限时的 `retry` |
|---|---|
| [`RedisStream::new`](../components/streams/index.md)、[`reclaim`](../components/streams/reclaim.md) | 向流追加一份副本 |
| [`RedisStream::claiming`](../components/streams/claiming.md) | 条目保持待处理，`min_idle` 之后回来 |
| [`RedisList::reliable`](../components/lists/reliable.md) | 条目移回列表 |
| [`RedisList::new`](../components/lists/index.md)、[`RedisPubSub`](../components/pubsub/index.md) | 什么也不做：消息丢失 |

声明了上限时，除认领订阅外，每种订阅上的 `retry` 都变成一份副本，这样次数随消息一起传递；认领的条目带有 Redis 自己的计数。

## 保存延迟的三种方式 {/* #three-ways-to-keep-a-delay */}

`retry_after` 结算这次投递，并要求在延迟之后再投递一次。
订阅用三种方式之一保存这个延迟。

框架自己的方式不需要在描述符上做任何设置。
运行时持有一份副本，延迟一到就把它发布回流的键。
副本带有重试次数消息头 [`RETRY_COUNT_HEADER`](api:ruststream::runtime::RETRY_COUNT_HEADER)，其值加一。
副本存在于进程中，所以在延迟结束前停止的服务会丢失它。

[`DelayedRetry::DurableZset`](../components/acknowledgement/delayed-retry.md) 把延迟保存在 Redis 中，`process_refund` 用的就是这种方式。
延迟的投递以到期时间为分值进入有序集合 `refunds.delayed`，重试次数加一，原条目被确认。
订阅在读取的同时，把每个到期的条目以新的 id 移回流中。
延迟在服务重启后依然有效。
每次读取执行一次清扫，所以延迟在到期时间之后的一个 [`block`](../components/subscriptions/block.md) 间隔内结束，默认是五秒。
分值是挂钟时间的毫秒数，所以服务各主机的时钟需要一致。
`ttl` 让被放弃的集合过期，它必须比最长的延迟还长，否则条目会在到期之前丢失。

[认领](../components/streams/claiming.md)订阅是第三种方式，它不需要集合。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/retries.rs#priority"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#priority"
```

</TabItem>
</Tabs>

重试把条目留在组的待处理列表中，空闲满 `min_idle` 后，订阅自己的下一次读取会把它取回。
不复制任何东西，条目保留原有的 id。
这种方式唯一能提供的等待时间就是 `min_idle`，所以更短的延迟会向上取到它，更长的延迟会被截短到它。
因此，`process_priority_refund` 把 `min_idle` 设为退避时间。
指定了 `DurableZset` 的认领订阅则按要求保存每个延迟。

## 上限与死信流 {/* #the-cap-and-the-dead-letter-stream */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/retries.rs#app"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#app"
```

</TabItem>
</Tabs>

[`max_attempts`](api:ruststream::runtime::RouterWith::max_attempts) 统计一笔退款的每次投递，第一次也算在内。
第五次投递不再重试：[`dead_letter`](api:ruststream::runtime::RouterWith::dead_letter) 把它发布到 `refunds.dlq`，载荷和消息头保持到达时的样子。
死信按退款到达的方式写入，所以来自流的退款落在流 `refunds.dlq` 上。
有上限而没有去向时，投递在达到上限时被拒绝；有去向而没有上限时，第一次重试就会转入死信。
被丢弃的退款完成结算，不会去任何地方；`refunds.dlq` 只存放用完了尝试次数的退款。

上限读取一个次数，次数从哪里来取决于订阅。
认领订阅读取 Redis 为条目保存的[投递次数](../components/acknowledgement/counts.md)。
这个次数包括停止的工作者经历的投递，所以导致工作者崩溃的退款也计入上限。
其他每种订阅读取运行时发布的副本上的重试次数消息头，延迟集合也会在每一轮把它加一。

重放条目的 [seek](../components/positions/seek.md) 会把它们作为新的投递再次发出。
Redis 从 1 开始重新计算这类条目的投递次数，所以认领订阅从重放时起计算上限；重试次数消息头不变。

## 读取死信 {/* #reading-the-dead-letters */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/retries.rs#dead_letters"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#dead_letters"
```

</TabItem>
</Tabs>

`refunds.dlq` 是一个普通的流，`review` 通过自己的组读取它。
经由副本传递的退款带有重试次数消息头，这里是 5。
`review` 把这笔退款记入日志，留给人来查看。

## 模式要指定重试去向 {/* #patterns-name-their-retry-destination */}

副本发往订阅报告的地址：流的键、列表的键或频道。
[Pub/Sub 模式](../components/pubsub/pattern.md)读取其通配模式匹配的每个频道，并不指定单个频道。
它的注册用 [`out_retry`](../components/acknowledgement/out-retry.md) 指定副本的去向，写作 `.out_retry(Publish::default()).to("events.retry")`，没有它服务无法启动。

## 测试延迟 {/* #testing-the-delays */}

示例文件中的测试在暂停的时钟上于进程内运行服务。
`tb.advance(BACKOFF)` 让 30 秒瞬间过去，延迟集合和认领订阅都是如此。
[测试 Redis 服务](testing.md)讲解了测试套件。
