---
sidebar_position: 3
description: "用消费者组在工作者和服务之间共享一个 Redis 流，并找回停止的工作者从未确认的条目。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 消费者组与崩溃恢复

本指南在工作者之间、服务之间共享一个 Redis 流，并找回停止的工作者没有处理完的条目。

示例是一个支付服务。
付款到达流 `payments`。
账簿组记录每笔付款，风控组审查每笔付款。

## 一组工作者 {/* #a-group-of-workers */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/consumer_groups.rs#ledger"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/consumer_groups.rs#ledger"
```

</TabItem>
</Tabs>

Redis 流要通过消费者组读取。
组在流中持有一个游标，游标之后的每个条目交给组内的一个消费者。
每个订阅是一个消费者，所以服务的每个运行实例都向 `ledger` 增加一个消费者，各实例分摊付款。
消费者得到一个生成的名字，在每个进程中依次为 `ruststream-0`、`ruststream-1` 等。
[`consumer`](api:ruststream_fred::stream::RedisStream::consumer) 可以为它命名，例如按主机命名，这样 `XINFO CONSUMERS` 就能区分各个实例。

交给消费者的条目进入组的待处理条目列表。
处理器确认它之后，它才离开这个列表。
如果工作者先停止了，无论是崩溃还是部署，条目都会保持待处理：Redis 保存着它，其他消费者也不会自动收到它。

[`RedisStream::claiming`](../components/streams/claiming.md) 在一次调用中读取两部分：先是待处理至少 `min_idle` 的条目，然后是新条目。
因此，接收新付款的订阅也接管停止的工作者留下的付款。
它需要 Redis 8.4 或更高版本；在更旧的服务器上服务无法启动，错误会指出订阅和两个版本号。

这个订阅上的 `retry` 不追加副本。
条目以原有的 id 保持待处理，空闲满 `min_idle` 后，下一次读取会认领它。
`post_payment` 用这一点处理正在迁移的账户：付款在待处理列表中等待，每 30 秒回来一次。

被认领的条目带有 [`DELIVERY_COUNT_HEADER`](../components/acknowledgement/counts.md)：这次之前的投递次数，新条目为零。
[`IDLE_MS_HEADER`](api:ruststream_fred::IDLE_MS_HEADER) 保存条目空闲了多久。
[重试、延迟与死信](retries.md)按这个次数限定投递。

## 选择 min_idle {/* #choosing-min_idle */}

`min_idle` 没有默认值。
空闲这么久的条目被视为已被放弃，所以把它设得比处理器最长的运行时间还长。
值太短，会从仍在处理的工作者手中拿走条目，付款就会被记录两次。
值太长，崩溃后的恢复就会推迟同样的时间。

## 第二个组 {/* #a-second-group */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/consumer_groups.rs#fraud"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/consumer_groups.rs#fraud"
```

</TabItem>
</Tabs>

每个组有自己的游标和自己的待处理列表，所以每个组都读取每个条目。
风控组收到账簿组收到的每笔付款，两者互不拖慢。
实践中，每个读取流的服务都有一个自己的组。

如果组还不存在，服务会在启动时创建它。
[`start_id`](../components/streams/stream-start.md) 指定新组从哪里开始：默认的 [`StreamStart::New`](api:ruststream_fred::stream::StreamStart::New) 只读取此后添加的条目，[`StreamStart::Beginning`](api:ruststream_fred::stream::StreamStart::Beginning) 读取流中仍保存的每个条目。
风控组晚于账簿组加入，所以它从开头开始，审查流中已有的付款。
已存在的组保留自己的游标，`start_id` 不会移动它；[start_at](../components/positions/start-at.md) 会。

## 服务 {/* #the-service */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/consumer_groups.rs#app"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/consumer_groups.rs#app"
```

</TabItem>
</Tabs>

`screen_payment` 在流 `payments.screened` 上以一个 `Screened` 作为回复。

## 按账户保持顺序 {/* #order-per-account */}

`workers(8, by_key)` 在八个分区上运行 `post_payment`。
分区键相同的付款进入同一个分区，依次处理，所以一个账户的付款按添加的顺序记录。
Redis 没有分区，所以键放在 [`PARTITION_KEY_HEADER`](api:ruststream_fred::PARTITION_KEY_HEADER) 消息头中。
生产者在发布时用 [`partition_key`](../components/publishing/partition-key.md) 步骤设置它。
没有键的付款进入任意分区。
顺序只在一个消费者内部成立；服务的两个实例各自记录自己分到的那部分付款。

## Redis 8.4 之前的恢复 {/* #recovery-before-redis-84 */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/consumer_groups.rs#legacy"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/consumer_groups.rs#legacy"
```

</TabItem>
</Tabs>

旧版服务器没有认领读取，所以恢复要靠同一个组上的第二个订阅。
[`RedisStream::new`](../components/streams/index.md) 读取新条目。
[`RedisStream::reclaim`](../components/streams/reclaim.md) 用 `XAUTOCLAIM` 只读取待处理至少 `min_idle` 的条目，从而回收停止的工作者持有的条目。
回收的投递会把自己计入 `DELIVERY_COUNT_HEADER`，所以第一次恢复报告的值至少为 1。

在 `new` 订阅上，`retry` 向流追加一份条目的副本，并确认原条目。
副本立即回来，排在期间添加的条目之后。
像上面迁移那样的等待，在这里要用 `retry_after`，[重试、延迟与死信](retries.md)讲解了它。

## 测试恢复 {/* #testing-recovery */}

示例文件中的测试在暂停的时钟上于进程内运行服务。
其中一个测试为正在迁移的账户发布一笔付款，用 `tb.advance` 把时钟推进 `min_idle`，然后看到账簿第二次收到这笔付款，而流上没有副本。
[测试 Redis 服务](testing.md)讲解了测试套件。
