---
sidebar_position: 1
description: "安装 ruststream-fred，启动本地 Redis，在 Redis 流上运行第一个订阅者，并在没有服务器的情况下测试它。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 快速开始

本指南安装 `ruststream-fred`，启动一个本地 Redis，并让第一个服务连上它运行。

## 安装 {/* #installation */}

```toml title="Cargo.toml"
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json", "logging"] }
ruststream-fred = "0.7"
serde = { version = "1", features = ["derive"] }
tracing = "0.1"

[dev-dependencies]
ruststream-fred = { version = "0.7", features = ["testing"] }
tokio = { version = "1", features = ["macros", "rt"] }
```

`ruststream` 是框架本身：`macros` 提供属性宏，`json` 提供默认编解码器，`logging` 把服务的日志打印到终端。
`ruststream-fred` 是 Redis Broker，基于 [fred](https://docs.rs/fred) 客户端构建。
它的 `testing` feature 加入测试所用的[进程内 Redis](../components/testing/index.md)。
`fred` 用 Rust 编写，所以构建不需要任何系统库。

这个 Broker crate 有以下 feature，默认全部关闭：

| Feature | 提供的内容 |
|---|---|
| `testing` | `TestApp` 背后的进程内 Redis |
| `asyncapi` | 生成的 AsyncAPI 文档中的 [Redis 扩展](../components/asyncapi/index.md) |
| `tls-rustls`、`tls-rustls-ring`、`tls-native-tls` | [TLS](../components/broker/tls.md)，分别基于使用 `aws-lc-rs` 的 rustls、使用 `ring` 的 rustls，或者系统的 TLS 库 |
| `sentinel-auth` | [Sentinel 节点](../components/broker/sentinel-credentials.md)单独使用的凭据 |
| `credential-provider` | 通过回调提供的[轮换凭据](../components/broker/credential-provider.md) |

## 本地 Redis {/* #a-local-redis */}

开发时，一个 Redis 服务器就够了：

```bash
docker run --name redis -p 6379:6379 redis:8.4
```

服务器监听 `localhost:6379`。
Redis 8.4 支持本 crate 的全部功能；[认领读取模式](../components/streams/claiming.md)至少需要这个版本。
Valkey 的用法相同：`RedisBroker::standalone` 接收 `valkey://` URL，TLS 则用 `valkeys://`。

## 第一个服务 {/* #the-first-service */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/getting_started.rs#service"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/getting_started.rs#service"
```

</TabItem>
</Tabs>

`take_order` 从 Redis 流 `orders` 读取 JSON 订单。
带金额的订单被确认，没有金额的订单被丢弃。
[`RedisBroker::standalone`](../components/broker/index.md) 接收服务器的 URL，只把它记录下来。
连接在服务启动时才打开。
Redis 流要通过消费者组读取。
[`RedisStream::new("orders").group("orders-svc")`](../components/streams/index.md) 指定流和组；如果两者还不存在，服务会在启动时创建它们。
`#[ruststream::app]` 把 `app` 变成 `main`；在手写形式中，[`run_main`](api:ruststream::runtime::cli::run_main) 做同样的事。

运行服务：

```bash
cargo run -- run
```

在另一个终端里，向流中添加一个订单：

```bash
docker exec redis redis-cli XADD orders '*' _payload '{"id":1,"amount_cents":1250}'
```

载荷放在条目的 `_payload` 字段中。
服务记录一条带订单 id 的 `order taken` 日志。
新建的组从流的末尾开始，所以服务读取的是它第一次启动之后添加的订单。
<kbd>Ctrl</kbd>+<kbd>C</kbd> 停止服务。

`cargo generate` 可以生成同类服务的脚手架，其中带有路由器和回复。
每种 Redis 结构各有一个模板：`redis-stream`、`redis-list` 和 `redis-pubsub`。

```bash
cargo generate --git https://github.com/powersemmi/ruststream-fred templates/redis-stream --name my-service
```

## 第一个测试 {/* #the-first-test */}

示例文件里还有两个测试。
每个测试都用 `TestApp::start` 启动同一个 `app`，发布一个订单，然后检查 `take_order` 如何结算它。
在测试中，`RedisBroker` 连接的是进程内 Redis，所以测试不需要服务器：

```bash
cargo test
```

[测试 Redis 服务](testing.md)逐一讲解一个更大服务的测试，以及进程内 Redis 模拟了什么。

## 从 fred 迁移 {/* #coming-from-fred */}

基于 `fred` 的服务要自己编写读取循环、确认和组的创建。
下表列出每一部分在 RustStream 中的位置：

| fred | RustStream |
|---|---|
| `Builder::from_config(..)` 和一个 `Pool` | [`RedisBroker::standalone`](../components/broker/index.md)、[`cluster`](../components/broker/cluster.md) 或 [`sentinel`](../components/broker/sentinel.md)，用 [`pool`](../components/broker/pool.md) 设置池的大小；[`from_pool`](../components/broker/from-pool.md) 接收你自己构建的池 |
| `XGROUP CREATE .. MKSTREAM` | 由订阅在启动时完成 |
| 一个 `XREADGROUP` 循环 | [`RedisStream`](../components/streams/index.md) 上的一个 `#[subscriber]` 处理器 |
| 解码条目的字段 | 处理器的参数类型，由编解码器解码 |
| 处理完成后的 `XACK` | 处理器的[结果](../components/acknowledgement/index.md) |
| 一个 `XAUTOCLAIM` 循环 | [`RedisStream::claiming`](../components/streams/claiming.md) 或 [`reclaim`](../components/streams/reclaim.md)（[消费者组与崩溃恢复](consumer-groups.md)） |
| 带 `LREM` 的 `BLMOVE` 工作者 | [`RedisList::reliable`](../components/lists/reliable.md)（[基于列表的工作队列](work-queues.md)） |
| 一个 `SubscriberClient` 及其消息流 | [`RedisPubSub`](../components/pubsub/index.md)（[用 Pub/Sub 广播](fan-out.md)） |
| `XADD`、`LPUSH`、`PUBLISH` | 回复或 `Out` 参数，经由[发布策略](../components/publishing/index.md)发送 |
| 包住一组 `XADD` 的 `MULTI` / `EXEC` | [事务](../components/transactions/index.md) |
