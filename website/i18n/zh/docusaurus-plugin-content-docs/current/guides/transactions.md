---
sidebar_position: 8
description: "用 MULTI 和 EXEC 把一组消息以全有或全无的方式发布到 Redis 流：一批的回复，或者由处理器持有的事务。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 事务

本指南以全有或全无的方式把一组消息发布到 Redis 流。

示例是一个订单服务。
它成批确认订单，并把每个打好的包裹按每件商品一次发货的方式发出。

## 一批的回复放在一个块中 {/* #a-batchs-replies-in-one-block */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/transactions.rs#confirm"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/transactions.rs#confirm"
```

</TabItem>
</Tabs>

事务把它的发布按发布顺序组成一个 `MULTI` 和 `EXEC` 块发送。
`confirmations` 的读者要么看到一批中的每条确认，要么一条也看不到。

`confirm` 是一个批处理器：它接收一个切片，为每个订单返回一条确认。
`Err` 不发布任何内容，并以它的结果结算整批。

## 挂载 {/* #the-mount */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/transactions.rs#app"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/transactions.rs#app"
```

</TabItem>
</Tabs>

`.out_reply(TransactionalPublish).transactional()` 要求使用[借用形式](../components/transactions/index.md)。
运行时在回复发布者上打开一个事务，把这批的回复发布进去，然后提交。
`TransactionalPublish` 是流发布策略 [`RedisPublish`](api:ruststream_fred::RedisPublish) 的另一个名字，它用 `XADD` 发送。

一批的大小就是拉取它的那次读取的 `COUNT`，所以这里一个事务最多包含 32 条确认。

## 处理器持有的事务 {/* #a-transaction-the-handler-owns */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/transactions.rs#ship"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/transactions.rs#ship"
```

</TabItem>
</Tabs>

`Out` 槽位上的 [`transaction()`](../components/transactions/transaction.md) 返回一个持有自己缓冲区的值。
它的发布进入缓冲区，`commit` 把缓冲区作为一个块发送，`abort` 丢弃缓冲区。
`commit` 和 `abort` 都会消耗这个值，所以第二次提交或结束后的发布无法编译。
一个发布者上可以同时打开多个持有型事务，结算其中一个从不影响其他事务。
既没有提交也没有中止就被丢弃的事务，按中止处理，并在日志中留下一条警告。

[`owned_transaction()`](../components/transactions/owned-transaction.md) 是同样的东西，只是针对值：每次发布接收一个去向和一个值，并用默认编解码器对值编码。
它适合在启动钩子中使用，例如在 `after_startup` 中为流写入初始数据。

## “没有回滚”的含义 {/* #what-no-rollback-means */}

Redis 把一个块中的命令排队，然后一起执行；它不会撤销这些命令。
服务器拒绝入队的命令，例如参数个数错误的命令，会让整个块作废，`commit` 返回一个错误。
在块执行过程中失败的命令，不会撤销它之前已执行的命令。
对于一组 `XADD`，这发生在服务器内存耗尽，或者某个键保存的是另一种类型的时候。

提交失败的事务已经被消耗，缓冲区也没有了。
处理器返回 `retry`，投递再次到来，并构建一个新的事务。

## 事务在哪里运行 {/* #where-transactions-run */}

事务在单机服务器和 Sentinel 部署上运行。
`MULTI` 块不能跨越哈希槽，所以在[集群](cluster.md)上打开任何一种事务都会返回错误。
列表和 Pub/Sub 的发布者没有事务。

事务覆盖的是发布。
在消费一侧，[`.pipeline().atomic()`](pipelining.md#atomic-segments) 把处理器自己的 Redis 命令和其投递的确认放在一个块中。

## 测试 {/* #testing */}

示例文件中的测试在进程内运行，[进程内 Redis](../components/testing/index.md) 会执行 `MULTI` 和 `EXEC`。
它们检查一批的确认和一个包裹的各次发货是一起到达的，并检查被丢弃的一批和被中止的包裹不发布任何内容。
