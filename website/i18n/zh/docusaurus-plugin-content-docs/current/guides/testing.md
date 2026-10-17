---
sidebar_position: 12
description: "用 TestApp 在进程内测试 Redis 服务：回复、处理器自己的 Redis 命令、暂停时钟上的延迟，以及连接服务器运行的同一个测试。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 测试 Redis 服务

测试用 [`TestApp`](api:ruststream::testing::TestApp) 运行服务自己的应用，连接的是[进程内 Redis](../components/testing/index.md) 或者服务器。

被测服务负责银行卡扣款。
付款到达流 `card-payments`，处理器在 `card-receipts` 上以一张收据作为回复。
没有金额的付款被丢弃。
每个账户每天最多花费 1,000.00：处理器通过 Broker 的[连接池](../components/context/fred-pool.md)把金额加到键 `spent:<account>` 上，超出限额的付款用 [`retry_after`](../components/acknowledgement/retry-after.md) 暂缓一分钟。
经过三次投递后，付款进入[死信](../components/acknowledgement/dead-letter.md)流 `card-payments.review`。
退款到达可靠列表 `card-refunds`，并把金额退回。
应用由 `service(url)` 构建，所以测试可以选择服务器，而 `main` 使用生产环境的服务器。
处理器通过 `fred` 的命令 trait 调用连接池，所以服务把 `fred` 列入依赖。

测试需要 `ruststream-fred` 的 `testing` feature 和一个 Tokio 运行时：

```toml title="Cargo.toml"
[dev-dependencies]
ruststream-fred = { version = "0.7", features = ["testing"] }
tokio = { version = "1", features = ["macros", "rt", "rt-multi-thread"] }
```

## 一笔付款及其收据 {/* #a-payment-and-its-receipt */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/testing.rs#receipt"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#receipt"
```

</TabItem>
</Tabs>

[`TestApp::start`](api:ruststream::testing::TestApp::start) 把 [`RedisBroker`](../components/broker/index.md) 连接到在测试进程内模拟的 Redis。
测试从不连接这个 URL。
测试按生产类型访问 Broker：`tb.broker::<RedisBroker>()`。
测试的消息发往 Broker 的默认发布者写入其名字的地方：对流执行 `XADD`，或者执行读取该名字的订阅所对应的 `LPUSH` 或 `PUBLISH`。
`publish` 在处理器及其回复都完成结算后才返回。
所以接下来的几行断言的是已经完成的状态：订阅者如何结算，以及什么到达了 `card-receipts`。
测试主体把已启动的应用作为参数接收，所以下面的 live 测试也运行它。

## 没有金额的付款 {/* #a-payment-without-an-amount */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/testing.rs#dropped"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#dropped"
```

</TabItem>
</Tabs>

`settled` 检查处理器返回的结果。
`assert_not_called` 检查没有发布任何收据。

## 处理器自己的命令与延迟 {/* #the-handlers-own-commands-and-a-delay */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/testing.rs#held"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#held"
```

</TabItem>
</Tabs>

处理器的 `INCRBY` 和 `DECRBY` 在进程内 Redis 上执行，本 crate 的每条命令都是如此。
第二笔付款让账户超出限额，所以它被暂缓。
退款是一个列表条目，它的处理器把额度退回。

测试在暂停的时钟上启动，[`advance`](api:ruststream::testing::TestApp::advance) 让时钟前进。
一分钟的 `HOLD` 不花费任何时间。
进程内 Redis 也读取测试的时钟，所以在时钟前进之前，条目 id、空闲时间和延迟集合的分值都保持不变。
`outcomes` 按顺序列出每次投递的结算方式；被暂缓的那次记录为 `Outcome::Nack`。

## 上限 {/* #the-cap */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/testing.rs#review"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#review"
```

</TabItem>
</Tabs>

超出每日限额的付款在每次投递时都被暂缓。
每次暂缓都把一份副本发布回流，重试次数加一，`published` 读回写入某个名字的每条内容，这些副本也包括在内。
第三次投递是挂载点允许的最后一次，所以这笔付款按到达时的样子进入 `card-payments.review`。

## 进程内 Redis 模拟了什么 {/* #what-the-in-process-redis-models */}

进程内 Redis 接收本 crate 发送的命令，并像 Redis 8.4 那样应答，所以[认领](../components/streams/claiming.md)订阅可以挂载。
它从 Broker 读取每项设置：URL、连接池大小、默认组和集群拓扑。
它维护：

- 键的类型，所以写入错误类型会返回 `WRONGTYPE` 错误；
- 消费者组及其游标、待处理条目和投递次数，`XAUTOCLAIM`、`XREADGROUP .. CLAIM` 和 [seek](../components/positions/seek.md)；
- 列表，包括可靠列表的处理中列表和它的恢复集合；
- Pub/Sub 频道、模式和分片频道，它们不为不在场的订阅者保存任何东西；
- 有序集合、键的过期和 `MULTI` / `EXEC`；
- 在集群拓扑下，拒绝跨越哈希槽的命令或事务。

处理器自己的命令，无论经由连接池还是[管道](../components/pipelining/index.md)轮次，也在它上面执行。
它不运行脚本：`EVAL`、`EVALSHA`、`FCALL` 以及它不认识的命令都会返回指出其名字的错误。
它不执行握手，所以认证和 TLS 都得不到检验，故障转移和重新分片也一样。
这些都需要服务器。

## 连接服务器运行同一个测试 {/* #the-same-test-against-a-server */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/testing.rs#live"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#live"
```

</TabItem>
</Tabs>

[`TestApp::start_live`](../components/testing/live.md) 把同一个应用连接到正在运行的 Redis。
这时，发布会在真实时钟上等待，直到每个订阅都处理完它，最多十秒。
测试用 `live::url`（示例自带的辅助函数）从 `REDIS_TEST_URL` 读取地址，未设置时跳过。
服务器保留着之前运行留下的数据，所以测试从一个专属账户中扣款。
断言与进程内的完全相同。

```bash
cargo test
docker run --name redis -p 6379:6379 redis:8.4
REDIS_TEST_URL=redis://localhost:6379 cargo test -- --test-threads=1
```
