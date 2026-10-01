# Redis Broker { #redis-broker }

`ruststream-fred` 让 [RustStream](https://powersemmi.github.io/ruststream/) 服务跑在 Redis 上。
Redis Streams 是一个日志，和 Kafka 一样：订阅通过消费者组读取它，并对处理过的每个条目做 ack。
列表和 Pub/Sub 也在这个 crate 里。开启 `testing` feature 后，测试在进程内模拟的 Redis 上运行服务
自己的应用，因此不需要 Redis 服务器。

```toml
ruststream = { version = ">=0.7.0-rc.10, <0.8.0", features = ["macros"] }
ruststream-fred = "0.7"
serde = { version = "1", features = ["derive"] }
```

`RedisBroker::standalone` 是同步的，不做 I/O：连接由运行时在启动时打开，在关闭时断开。

发布策略你在注册处理器时指定：流用 `RedisPublish`，频道用 `RedisPubSubPublish`，列表用
`RedisListPublish`。运行时按这个策略在已连接的 Broker 上实例化发布者，因此发布无法早于连接发生。

策略覆盖整次发布，只留一项设置给消息自己。`XADD`、`LPUSH` 和 `PUBLISH` 接收键或频道以及值，因此
处理器函数体通常只写 `.message(&value).publish()`，别的什么都不写；例外是分区键，它是发布构建器上
的一个步骤。设置它的函数体导入本 crate 的 prelude，并在自己的约束里写出设置类型；见[分区键](https://docs.rs/ruststream-fred/latest/ruststream_fred/index.html#partition-keys)。

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

## 其余内容在哪里 { #where-the-rest-is }

docs.rs 上的参考文档以这个 crate 自己的教程开篇，一个主题一节：

- [订阅](https://docs.rs/ruststream-fred/latest/ruststream_fred/index.html#subscribing)：各个描述符
  以及它们各自对重新投递的回答，按传输划分（[流](https://docs.rs/ruststream-fred/latest/ruststream_fred/index.html#streams)、
  [列表](https://docs.rs/ruststream-fred/latest/ruststream_fred/index.html#lists)、
  [Pub/Sub](https://docs.rs/ruststream-fred/latest/ruststream_fred/index.html#pubsub)），还有
  [批](https://docs.rs/ruststream-fred/latest/ruststream_fred/index.html#batches)、
  [原生投递字段](https://docs.rs/ruststream-fred/latest/ruststream_fred/index.html#native-delivery-fields)、
  [延迟重新投递](https://docs.rs/ruststream-fred/latest/ruststream_fred/index.html#delayed-retry)、
  [投递次数上限](https://docs.rs/ruststream-fred/latest/ruststream_fred/index.html#capping-the-retries)
  和[消费者组重新定位](https://docs.rs/ruststream-fred/latest/ruststream_fred/index.html#repositioning-a-group)。
- [管道](https://docs.rs/ruststream-fred/latest/ruststream_fred/index.html#pipelining)：订阅的窗口一次往返确认整批拉取的消息，
  处理函数也可以把 Redis 命令排入这个窗口，只有投递被确认时这些命令才会执行。
- [发布](https://docs.rs/ruststream-fred/latest/ruststream_fred/index.html#publishing)：各个策略、
  [分区键](https://docs.rs/ruststream-fred/latest/ruststream_fred/index.html#partition-keys)步骤和
  [事务](https://docs.rs/ruststream-fred/latest/ruststream_fred/index.html#transactions)。
- [生成的文档](https://docs.rs/ruststream-fred/latest/ruststream_fred/index.html#the-generated-document)：
  AsyncAPI 文档关于 Redis 频道说了什么，又刻意不说什么。
- [测试](https://docs.rs/ruststream-fred/latest/ruststream_fred/index.html#testing)：在进程内的
  Redis 上运行服务自己的应用，模型保留了什么，以及什么该留给真实服务器上的测试。
- [运维](https://docs.rs/ruststream-fred/latest/ruststream_fred/index.html#operations)：拓扑、凭据、
  TLS 和已知限制。

处理器、路由器、编解码器和中间件都来自框架本身，它的入口页面从
[RustStream 站点](https://powersemmi.github.io/ruststream/)开始。
