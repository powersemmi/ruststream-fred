# 重试次数上限 { #capping-the-retries }

一个不停要求重试的处理器，会让自己的消息一直转下去，直到有人来干预。挂载点上的两个步骤终结这件
事，而且在这个 crate 的每一种传输上写法都一样：

```rust
--8<-- "crates/ruststream-fred/examples/fred_dead_letter.rs:app"
```

`max_attempts(n)` 是一条消息能得到的投递次数，含第一次。`dead_letter(name)` 是次数用尽之后这次投
递的去处：消息按到达时的样子发布到那里，负载和消息头都保持原样。只声明上限而不声明去处，就是拒绝
这次投递；只声明去处而不声明上限，则从第一次重试起就把消息带走。

处理器自己什么都不数：

```rust
--8<-- "crates/ruststream-fred/examples/fred_dead_letter.rs:handler"
```

可靠模式的列表读起来一样，去处写成一个列表键：

```rust
--8<-- "crates/ruststream-fred/examples/fred_list_dead_letter.rs:handler"
```

## 上限读的是哪个计数 { #which-count-the-cap-reads }

流的两种会认领条目的读取模式，`reclaim` 和 `claiming`，报告的是 Redis 记在待处理条目列表里的投递次
数。一条被崩溃的消费者取走的消息，在被重新认领回来时就计入上限，哪怕这个进程里没人见过它失败。

其余订阅没有自己的计数，因此上限读的是框架的重试次数消息头，它随运行时发布的副本一起走。上限之下
的立即重试于是变成一份这样的副本，而不是一次普通的 `nack`，计数也就跟着消息一起走。ZSET
[延迟队列](streams.md#delayed-retry)重放条目时也把同一个消息头加一，所以上限同样会数上它跑的这
几轮。

`claiming` 的投递还带着 `DELIVERY_COUNT_HEADER` 和 `IDLE_MS_HEADER`：它们报告的是本次投递之前待处
理条目列表的状态。上限所读的计数把本次投递也算进去，因此在 `claiming` 上它比消息头大一，而在回收
来的投递上与消息头相等。

## 重试副本发到哪里 { #where-a-retry-copy-goes }

只要 Redis 没有自己的重新投递可用，副本就由本进程发布，因此每个描述符都说明自己的副本发往何处。

| 订阅 | 副本发布到哪里 |
|---|---|
| `RedisStream`，任意读取模式 | 流的键：发到那里的 `XADD` 由消费者组读取 |
| `RedisList` | 列表的键 |
| `RedisPubSub` | 频道 |
| `RedisPubSubPattern` | 描述符说不出的地方：由你来指定 |

通配模式订阅读取它匹配到的每一个频道，而通配模式并不是 `PUBLISH` 能写出的频道。因此挂在它上面的
注册自己指定去处：`.out_retry(policy).to("events.retry")`，或者用一个读取每次投递所在频道的发布变
换。两者都不写的注册会拒绝启动。

`reclaim` 订阅是唯一一处副本到达的是消费者组、而不是发出副本的那个订阅：`XAUTOCLAIM` 只交出已经在
另一个消费者名下待处理的条目，因此新条目会被旁边读取新鲜尾部的消费者读到。这个模式本来就是为这样
的拓扑写的。

上限里写的去处是同一个 Broker 上的普通名字，生成的 [AsyncAPI 文档](asyncapi.md)把它显示为服务发布
的一个频道。
