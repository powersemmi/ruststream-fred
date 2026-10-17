---
sidebar_position: 7
description: "把处理器的 Redis 写入与确认放在同一次往返中发送，只在投递被确认时发送，或者与确认一起组成一个 MULTI 块。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 随确认一起执行的副作用

本指南把处理器自己的 Redis 写入与其投递的确认一起发送。

示例是一个订单服务。
它把每个订单的各行放入仓库的拣货队列，以一张带编号的收据作为回复，并把付款记入账簿。

## 排队一条命令 {/* #queue-a-command */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/pipelining.rs#accept"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/pipelining.rs#accept"
```

</TabItem>
</Tabs>

先写入 Redis 再确认的处理器要付出两次往返。
如果在两者之间崩溃，写入已经生效而确认没有发出，条目会被再次投递。

`Ctx<keys::Pipeline>` 把投递的[轮次](../components/pipelining/index.md)交给处理器，它是一个 [`RedisPipeline`](api:ruststream_fred::pipeline::RedisPipeline)。
它的[命令方法](../components/pipelining/commands.md)就是 `fred` 的那些：`lpush`、`hincrby`、`xadd`、`publish`、`eval` 等等。
调用只把命令放入队列，并立即返回。
`Ok` 表示命令已入队，而不是 Redis 已经执行了它。

入队的命令在处理器返回之后发出，而且只在处理器确认投递时才发出。
它们先发出，然后是 `XACK`，全部在一个管道中。

| 结果 | 入队的命令 | 结算 |
| --- | --- | --- |
| `ack` | 发送，在结算之前 | `XACK` |
| `drop` | 丢弃 | `XACK` |
| `retry`、`retry_after` | 丢弃 | 重试副本，然后 `XACK` |
| 未结算 | 丢弃 | 无：条目保持待处理 |

`accept` 为每一行排队一次推入。
缺少数量的行会让整个订单被丢弃，为前面各行排队的推入也随之丢弃。

## 立即读取一个值 {/* #read-a-value-now */}

入队的命令在处理器内部没有应答。
处理器立即需要的值，要通过 Broker 的连接池获取，[`Ctx<keys::FredPool>`](../components/context/fred-pool.md) 提供这个池。
`accept` 就是这样用 `INCR` 为每张收据编号的。
与 `keys::Pipeline` 一起使用时，`keys::FredPool` 在签名中排在第二位。

## 轮次中的回复 {/* #replies-in-the-round */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/pipelining.rs#app"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/pipelining.rs#app"
```

</TabItem>
</Tabs>

挂载点上的 `.pipeline()` 打开窗口，只有用它挂载的订阅才能让处理器的命令入队。
接收 `Ctx<keys::Pipeline>`、却在没有这一步的情况下挂载的处理器无法编译。

没有它时，回复在处理器返回后立即发布，先于轮次发出。
回复上的 [`InRound`](../components/pipelining/in-round.md) 变换把回复放入轮次，排在命令之后，所以收据只与拣货任务一起发出。
处理器用 `pipeline.bind(out)` [绑定](../components/pipelining/bind.md) `Out` 槽位之后，经由该槽位的发布也会加入轮次。

窗口收集一次读取中每个投递的轮次。
这次读取的投递全部结算之后，窗口才发出，所以在负载下一次往返就能带走整批拉取的条目。
在消息稀少时，最后一个处理器一返回，窗口就发出。
发送失败只报告一次，带有订阅以及它携带的命令数和结算数。
`XACK` 失败的条目保持待处理，会被再次投递。

批处理器从自己的上下文 `ctx.context(keys::Pipeline)` 取得轮次，只有当批中的每个条目都被确认时，它的命令才会发出（[Flush](../components/pipelining/flush.md)）。

## 原子段 {/* #atomic-segments */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/pipelining.rs#capture"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/pipelining.rs#capture"
```

</TabItem>
</Tabs>

管道不是事务：一条命令可能成功，而它之后的 `XACK` 却失败了。
`.pipeline()` 之后的 [`.atomic()`](../components/pipelining/atomic.md) 把每个投递的命令及其结算包在 `MULTI` 和 `EXEC` 中。
这样，一笔付款的账簿条目、余额和确认要么一起生效，要么都不生效。

Redis 没有回滚。
在 `EXEC` 中失败的命令，例如对另一种类型的键执行 `HINCRBY`，不会撤销其他已执行的命令。

在[集群](cluster.md)上，`MULTI` 块在一个哈希槽上运行。
流是 `{payments}`，处理器的键是 `{payments}:ledger` 和 `{payments}:balances`：花括号中的哈希标签把它们都放在流所在的槽上。
流的名字带有花括号，所以付款类型不声明去向，由生产者在发布时指定流。

## Lua 脚本 {/* #lua-scripts */}

`eval` 和 `evalsha` 像其他命令一样让 Lua 脚本入队，脚本在轮次中与结算一起运行。
脚本在服务器上原子地运行，所以适合不能拆开的一次检查加一次写入。
[进程内 Redis](../components/testing/index.md) 不运行脚本，所以测试让脚本入队的处理器时，要连接服务器运行。

## 测试 {/* #testing */}

示例文件中的测试在进程内发布一个订单、一个被丢弃的订单和一笔付款。
它们读回拣货任务、收据和账簿条目，并检查被丢弃的订单不会发出它排队的任何内容。
