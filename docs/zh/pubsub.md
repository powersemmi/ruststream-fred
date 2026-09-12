# Pub/Sub { #pubsub }

Pub/Sub 发完即忘：消息只到达那一刻连着的订阅者，而 `ack` 和 `nack` 返回 `Unsupported`。

用 Pub/Sub 的服务导入 `ruststream_fred::pubsub::prelude::*`：描述符、它的模式，以及这种形式的发布
策略，名字是 `Publish`。

`RedisPubSub` 描述符写明频道和模式。经典投递到达集群的每个节点，而 `.pattern()` 订阅的是频道模式，
不是单个频道：

```rust
--8<-- "crates/ruststream-fred/examples/fred_pubsub.rs:classic"
```

分片投递（`SSUBSCRIBE`，Redis 7+）留在槽内，因此能在集群上横向扩展，并且不接受模式。
`.mode(PubSubMode::Sharded)` 按订阅选中它：

```rust
--8<-- "crates/ruststream-fred/examples/fred_pubsub.rs:sharded"
```

一个服务可以同时在单机服务器上跑经典 Pub/Sub，在集群上跑分片 Pub/Sub；每个处理器挂在自己的 Broker
上：

```rust
--8<-- "crates/ruststream-fred/examples/fred_pubsub.rs:app"
```

这种形式的策略你在挂载处理器的地方指定：`.out(Reply, Publish)`，再加 `.mode(PubSubMode::Sharded)`
去对上分片的订阅者。`Reply` 是该策略绑定的位置，也就是处理器返回的那个值。这个策略用 `PUBLISH`
发出应答，而不是用 Broker 默认发布者的 `XADD`。

应答发往哪个频道由应答类型决定：上面的 `AuditEntry` 声明了 `audit`。不声明频道的应答类型，发往订阅者
的 `publish("..")` 写明的地方。

Pub/Sub 一次投递一条消息，因此批次由订阅者自己攒。批量处理器在挂载处用 `batch(n)` 写明大小，看到的
批次绝不会更长（见[批次](streams.md#batches)）。

一次发布把消息头和载荷一起封帧。默认的帧是二进制的。在订阅者和发布者上设定同一个编解码器
（`.codec(JsonCodec)`）会换成 `{headers, payload}` 信封，由编解码器序列化，值在 RedisInsight 这类
工具里因此可读。两种帧都原样装载任意字节：在信封里，合法 UTF-8 的字段写成文本，其他字节按原样写入。
外部客户端发布的值，到达时是载荷，没有消息头。
