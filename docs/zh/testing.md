# 测试 { #testing }

`testing` feature 提供 `RedisTestBroker`，一个按流的键精确路由的进程内传输，不需要服务器。它复现
路由、确认和消息头。它不复现消费者组的游标、`XAUTOCLAIM` 重新投递、裁剪和死信路由，这些属于对着
真实服务器写的测试。

```toml
[dev-dependencies]
ruststream-fred = { version = "0.7", features = ["testing"] }
```

## 对处理器做单元测试 { #unit-testing-a-handler }

`#[subscriber]` 处理器在 `RustStream` 应用内部运行，因此测试围绕 `RedisTestBroker` 搭出同样的应用，
交给 `TestApp`。经由测试套件的句柄发布，会把整个反应推进到静止，因此其后的断言不必等待。

### 业务逻辑测试 { #business-logic-test }

处理器校验自己的输入，把合法的经由仓储连接器保存下来，其余的丢弃。它里面没有任何东西知道测试套件
的存在。

```rust
--8<-- "crates/ruststream-fred/examples/fred_testing.rs:repository"
```

```rust
--8<-- "crates/ruststream-fred/examples/fred_testing.rs:business-handler"
```

测试发布一笔合法的支付和一笔非法的支付，然后断言只有合法的那笔存了下来：

```rust
--8<-- "crates/ruststream-fred/examples/fred_testing.rs:business-test"
```

在你自己的 crate 里，同样的函数体放进 `#[cfg(test)]` 模块中的 `#[tokio::test]`：

```rust
--8<-- "crates/ruststream-fred/examples/fred_testing.rs:unit-test"
```

### 各传输的例子 { #transport-specific-examples }

=== "Redis Stream"

    ```rust
    --8<-- "crates/ruststream-fred/examples/fred_testing.rs:stream-handler"
    ```

    ```rust
    --8<-- "crates/ruststream-fred/examples/fred_testing.rs:stream-test"
    ```

=== "Redis List"

    ```rust
    --8<-- "crates/ruststream-fred/examples/fred_testing.rs:list-handler"
    ```

    ```rust
    --8<-- "crates/ruststream-fred/examples/fred_testing.rs:list-test"
    ```

=== "Pub/Sub"

    ```rust
    --8<-- "crates/ruststream-fred/examples/fred_testing.rs:pubsub-handler"
    ```

    ```rust
    --8<-- "crates/ruststream-fred/examples/fred_testing.rs:pubsub-test"
    ```

## 这个替身挂载什么、忽略什么、拒绝什么 { #what-the-stand-in-mounts-ignores-and-refuses }

本 crate 提供的每个描述符和每个发布策略都能挂到 `RedisTestBroker` 上，因此测试接的就是服务发布出去
的那份声明。上面三个处理器写的是 `RedisStream`、`RedisList` 和 `RedisPubSub`，和路由文件里的写法
一致，没有裸的键字符串，挂载点上也没有重新映射。应答那一半也是生产代码用的同一个值：

```rust
--8<-- "crates/ruststream-fred/examples/fred_testing.rs:reply-handler"
```

```rust
--8<-- "crates/ruststream-fred/examples/fred_testing.rs:reply-test"
```

各形式 prelude 里的 `Publish` 在已连接的测试 Broker 上实例化发布者，和在真实 Broker 上一样，它同时
也是这个替身的默认应答发布者，因此不指定策略就应答的处理器在这里照样能用。没有只给测试用的发布策略。

这个替身只按键或频道路由，别的一概不看。消费者名和组名、`start_id`、`block`、`dead_letter`、
`max_deliveries`、`delayed_retry`、列表 `reliable` 模式的处理中列表和孤儿恢复看门狗、Pub/Sub 的
`mode` 以及信封的 `codec`，在进程内什么都不改变。发布侧也一样：列表的 `ttl` 没有键可以过期，信封的
编解码器也从不运行，因此应答在这里读回来是裸载荷，在真实服务器上则是一个帧。这些都要对着真实服务器
断言。

能力按形式对齐。流的策略实例化出的发布者带着两种事务，和 `RedisPublisher` 一样；列表和 Pub/Sub 的
策略实例化出 `RedisTestPlainPublisher`，它只提供 `Publisher`，和 `RedisListPublisher`、
`RedisPubSubPublisher` 一样。约束在事务能力上的槽位，在进程内编译不过的地方，正是它对着真实服务器
编译不过的地方，而不是在测试套件下通过、到生产构建时才崩。

有两种配置错误在这里就能抓到，抓法和 Redis 一样：没有消费者组的流，以及没有 `min_idle` 的列表恢复
ZSET。这个替身还直接拒绝两个描述符，因为在进程内照办会投递出真实订阅从不投递的东西：

- `RedisStream::reclaim(..)` 读的是另一个消费者陈旧的待处理条目。这个替身不保留待处理列表，因此这样
  挂载只会把新条目交给处理器。
- `RedisPubSub::pattern()` 订阅的是一个通配模式。这个替身精确匹配频道名，因此这样挂载会在通配模式
  本该捕获的每个频道上保持沉默。

结算随传输而定。流和可靠模式的列表会确认，重新入队会重新投递；Pub/Sub 和简单模式的列表返回
`AckError::Unsupported` 并拒绝重新入队，这里和真实服务器上完全一样。比 `shutdown` 活得更久的句柄会
返回 `RedisError::ShutDown`，而不是往没人读的路由器里写。

## 对着真实服务器 { #against-a-real-server }

本 crate 自己的实时测试集在 `tests/integration_fred.rs`，覆盖进程内传输做不到的部分：真实的消费者组、
`XACK`、nack 后重新发布这条路径、`XAUTOCLAIM` 回收，以及集群和 sentinel 拓扑。每种拓扑各由自己的
环境变量开关，因此一个都不设时 `cargo test` 不需要服务器。`docker-compose.test.yml` 把三种都拉起来，
`just test-brokers` 启动它们并跑这个测试集。
