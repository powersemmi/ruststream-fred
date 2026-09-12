# Redis 列表（工作队列） { #redis-lists-work-queue }

Redis 的列表是一个带竞争消费者的工作队列：条目用 `LPUSH` 放进去，消费者从右边弹出，并且恰好只有
一个消费者拿到它。

用列表的服务导入 `ruststream_fred::list::prelude::*`：描述符，以及这种形式的发布策略，名字是
`Publish`。

简单模式用 `BRPOP` 弹出，不做任何结算：崩溃会丢掉这个条目（至多一次）：

```rust
--8<-- "crates/ruststream-fred/examples/fred_list.rs:simple"
```

可靠模式把条目移进一个处理中列表，等处理器 ack 时再删掉，因此崩溃意味着这件活儿重做一次，而不是
丢失（至少一次）：

```rust
--8<-- "crates/ruststream-fred/examples/fred_list.rs:reliable"
```

可靠模式下处理器拒绝掉的条目，在 nack 要求重新入队时回到主列表，任何消费者都能从那里取走它。
不带重新入队的 nack 丢弃这个条目，这件活儿不会再跑一次。

`RedisListPublish` 策略用 `LPUSH` 发布。你在挂载处理器的地方指定它，运行时按它在已连接的 Broker 上
实例化发布者；在应用之外，`connected.list_publisher(RedisListPublish::new())` 直接返回一个。

列表条目的封帧方式和 [Pub/Sub](pubsub.md) 消息一样：一个可以装任意字节的二进制帧，或者两端设定同一个
编解码器（`.codec(JsonCodec)`）时由编解码器序列化的信封。

一次弹出返回一个条目，因此批次由订阅者自己攒。批量处理器在挂载处用 `batch(n)` 写明大小，看到的批次
绝不会更长；`block(..)` 接在大小之后链下去，和在流上一样（见[批次](streams.md#batches)）。

## 孤儿条目恢复 { #orphan-recovery }

消费者在处理器执行到一半时死掉，会把自己的条目留在处理中列表上，因为 Redis 的列表不记录任何待处理
状态。写明一个 ZSET 键就打开恢复看门狗，它默认关闭：

```rust
--8<-- "crates/ruststream-fred/examples/fred_list.rs:recovery"
```

订阅把每次认领以认领时间记进 ZSET，并在读取的同时扫这个 ZSET。空闲超过 `min_idle` 的条目回到主列表，
在那里由活着的消费者再次取走。把 `min_idle` 设得比处理器最长的运行时间还长：设短了会恢复一个仍在
处理中的条目，这件活儿于是跑两次。`recovery_ttl` 让被遗弃的 ZSET 键过期，它必须比 `min_idle` 长。
想要一个自己就能恢复的持久队列，就用 Redis Streams。

## 列表发布者的 TTL { #list-publisher-ttl }

列表键上的 TTL 给闲置的队列设上界：`RedisListPublish::new().ttl(Duration::from_secs(300))` 在每次
发布时重新挂上一个 `PEXPIRE`，因此在用的队列永远不过期，闲置的则会消失。TTL 默认关闭，并且覆盖整个
列表：Redis 的列表没有逐条目的过期。
