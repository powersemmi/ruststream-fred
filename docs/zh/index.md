# Redis Broker { #redis-broker }

`ruststream-fred` 让 [RustStream](https://powersemmi.github.io/ruststream/) 服务跑在 Redis 上。
Redis Streams 是一个日志，和 Kafka 一样：订阅通过消费者组读取它，并对处理过的每个条目做 ack。
列表和 Pub/Sub 也在这个 crate 里，`testing` feature 还提供一个进程内的测试 Broker，测试因此不需要
Redis 服务器。

```toml
ruststream = { version = "0.7", features = ["macros"] }
ruststream-fred = "0.7"
serde = { version = "1", features = ["derive"] }
```

`RedisBroker::standalone` 是同步的，不做 I/O：连接由运行时在启动时打开，在关闭时断开。

发布策略你在注册处理器时指定：流用 `RedisPublish`，频道用 `RedisPubSubPublish`，列表用
`RedisListPublish`。运行时按这个策略在已连接的 Broker 上实例化发布者，因此发布无法早于连接发生。

策略覆盖整次发布，只留一项设置给消息自己。`XADD`、`LPUSH` 和 `PUBLISH` 接收键或频道以及值，因此
处理器函数体通常只写 `.message(&value).publish()`，别的什么都不写；例外是分区键，它是发布构建器上
的一个步骤。设置它的函数体导入本 crate 的 prelude，并在自己的约束里写出设置类型；见[分区键](streams.md#partition-keys)。

## 生成服务骨架 { #scaffold-a-service }

用 [`cargo generate`](https://github.com/cargo-generate/cargo-generate) 生成一个能直接跑的起步项目，
每种传输一个模板：

```bash
cargo generate --git https://github.com/powersemmi/ruststream-fred templates/redis-stream
cargo generate --git https://github.com/powersemmi/ruststream-fred templates/redis-pubsub
cargo generate --git https://github.com/powersemmi/ruststream-fred templates/redis-list
```

## 拓扑 { #topologies }

拓扑由三个具名构造函数选定：

```rust
--8<-- "crates/ruststream-fred/examples/fred_topologies.rs:topologies"
```

## 传输指南 { #transport-guides }

- [Redis Streams](streams.md) - 消费者组、读新条目还是回收、批次、重新定位、延迟重新投递。
- [Redis 列表](lists.md) - 竞争消费者的工作队列、可靠模式、孤儿条目恢复。
- [Pub/Sub](pubsub.md) - 经典广播和分片广播。
- [死信与投递次数上限](dead-letter.md) - 给无休止的重新投递设上界。
- [认证与 TLS](auth-tls.md) - 每种拓扑上的凭据和 TLS。
- [事务](transactions.md) - standalone 和 sentinel 上的批量发布。
- [测试](testing.md) - 在进程内运行服务和它的处理器，不需要 Redis 服务器。
