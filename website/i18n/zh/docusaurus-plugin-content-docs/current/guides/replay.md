---
sidebar_position: 9
description: "每次启动时从最旧的条目重放 Redis 流，在处理器中把消费者组跳到末尾，并了解 seek 不影响哪些东西。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 重放与 seek 流

本指南移动消费者组在 Redis 流上的游标：向后移动以重放条目，向前移动以跳过条目。

示例是一个支付服务。
它从账簿流重建余额视图，并从另一个流向客户发送提醒。

## 从头重放 {/* #replay-from-the-beginning */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/replay.rs#rebuild"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/replay.rs#rebuild"
```

</TabItem>
</Tabs>

条目被读取和确认之后，Redis 流仍然保存它们。
消费者组在这些条目上维护一个游标：组收到的最后一个条目的 id。
重放就是把这个游标往回移，手动操作是 `XGROUP SETID`。

[`start_at`](../components/positions/start-at.md) 在每次启动时、订阅的第一次投递之前移动游标。
[`RedisGroupPosition::beginning`](../components/positions/index.md) 是流仍保留的最旧条目，所以服务每次启动时，`rebuild` 都读取整个账簿。
这样重建的视图从空状态开始，例如保存在内存中的状态。

游标属于组，而不属于订阅。
`balances` 的每个消费者都会重放，无论在这个服务中，还是在它的其他每个实例中。

## 挂载 {/* #the-mount */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/replay.rs#app"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/replay.rs#app"
```

</TabItem>
</Tabs>

使用宏时，`start_at(..)` 是 `#[subscriber]` 的一个子句。
在手写形式中，它是挂载的一个步骤，位于 `build()` 之后。

| 位置 | 组从哪里继续 |
| --- | --- |
| `RedisGroupPosition::beginning()` | 流保留的最旧条目 |
| `RedisGroupPosition::end()` | 末尾：只读取之后添加的条目 |
| `RedisGroupPosition::after(id)` | `id` 之后的条目，`id` 本身被跳过 |

[`StreamStart`](../components/streams/stream-start.md) 是另一项设置：它指定组还不存在时从哪里开始。
`start_at` 在每次启动时移动一个已存在的组。

## 跳到末尾 {/* #skip-to-the-tail */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/replay.rs#skip"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/replay.rs#skip"
```

</TabItem>
</Tabs>

服务运行期间，处理器通过 [seek 句柄](../components/positions/seek.md)移动自己的组。
`Ctx<keys::SeekHandle>` 提供这个句柄；它的 `group()` 指出它移动的是哪个组。

某个生产者误向 `alerts` 流灌入大量提醒，运维人员趁提醒仍在涌入时向流中追加一个停止标记。
`notify` 读到标记后，把组移到末尾，所以流中位于标记之后的提醒永远不会发出。
seek 一返回就生效。
旧游标已经选中的条目会被丢弃而不投递，阻塞在 `XREADGROUP` 中的订阅会在一个 `block` 间隔内按新游标读取。

批处理器在自己的批上下文上用 `ctx.context(keys::SeekHandle)` 读取同一个句柄。

## 重放一次投递 {/* #replay-one-delivery */}

投递知道自己的位置：[`Ctx<keys::Position>`](../components/positions/seek.md) 是重新投递它的游标。
seek 到那里，会再次投递同一个条目，然后按流的顺序投递它之后的条目。
需要在临时故障时保持顺序的处理器，可以 seek 到那里，而不是返回 `retry`，因为在流上 `retry` 会在末尾追加一份副本。
[`Ctx<keys::EntryId>`](../components/positions/entry-id.md) 是条目自己的 id，`RedisGroupPosition::after(id)` 用保存在别处的 id 构建一个位置。

## seek 不影响的东西 {/* #what-a-seek-leaves-alone */}

seek 只移动游标，不做别的。

- 已投递但未确认的条目留在组的待处理条目列表中，[回收](../components/streams/reclaim.md)订阅或[认领](../components/streams/claiming.md)订阅仍然能找到它。
- 在[延迟](../components/acknowledgement/delayed-retry.md)集合中等待的副本，到期时照常追加，不管组在哪里读取。
- 重放的条目会作为一次新的投递读取，Redis 从 1 开始重新计算它的投递次数。认领订阅从重放时起计算上限，框架自己的重试次数不变。

## 保留 {/* #retention */}

Redis 一直保存流的条目，直到有什么东西裁剪它们，重放只能到达仍保存的条目。
用 `XTRIM` 裁剪由服务决定：处理器通过 [`Ctx<keys::FredPool>`](../components/context/fred-pool.md) 发送它，或者由运维人员执行。
游标指向最旧保留条目之前的组，会从最旧的条目开始读取。

## 测试 {/* #testing */}

示例文件中的测试在进程内运行。
它们检查视图读取了每个条目，以及停止标记跳过了排在它后面的提醒。
跨重启的重放需要一个在两次启动之间保留流的服务器，所以这个测试连接 `REDIS_TEST_URL` 以 live 方式运行。
