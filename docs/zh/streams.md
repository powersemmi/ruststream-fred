# Redis Streams { #redis-streams }

Redis 的流是一个日志：条目一直留在里面，直到流做一次裁剪；消费者组把每个条目交给自己的一个消费者，
并记住哪些已经 ack 过。

用流的服务导入 `ruststream_fred::stream::prelude::*`：描述符、定位类型连同承载它们的上下文，以及
这种形式的发布策略，名字是 `Publish`。`TransactionalPublish` 是同一个策略的事务名字，因为流的发布者
本来就在句柄上缓冲。

处理器文件改为导入 `ruststream::prelude::*`，只写出注入的发布者需要的那项能力
（`Out<impl Publisher>`、`Out<impl TransactionalPublisher>`），于是把处理器换一种形式时改的是描述符，
不是处理器。

`#[subscriber("key")]` 处理器读取该键下的流。每次读取都走消费者组，因此裸字符串这种形式需要一个
Broker 级别的默认组（`.default_group`）：

```rust
--8<-- "crates/ruststream-fred/examples/fred_streams.rs:handler"
```

把它挂到 Broker 上：

```rust
--8<-- "crates/ruststream-fred/examples/fred_streams.rs:app"
```

条目把载荷放在一个保留字段里，每个消息头放在 `h:` 前缀下，因此 `XADD` 和 `XREADGROUP` 两者都保留。

## 读取模式：读新条目还是回收 { #read-modes-fresh-tail-vs-reclaim }

读取模式由构造函数选定，因为这两种模式返回的条目集合互不相交：

- `RedisStream::new(key)` 从流的末尾读新条目（`XREADGROUP >`）。这是普通的工作订阅者。
- `RedisStream::reclaim(key, min_idle)` 回收另一个消费者取走却没有 ack 的条目（`XAUTOCLAIM`，空闲
  至少 `min_idle`）。这是崩溃恢复，和同一个组里的 `new` 订阅者并排运行（“每组两个处理器”）。

`min_idle` 没有默认值。把它设得比处理器最长的运行时间还长，否则回收会拿走健康消费者还在处理的
消息，这条消息于是处理两次。

描述符可以写在 `#[subscriber(..)]` 属性里。读新条目的工作订阅者：

```rust
--8<-- "crates/ruststream-fred/examples/fred_reclaim.rs:worker"
```

同一个组里做恢复的处理器，回收空闲超过 30 秒的条目：

```rust
--8<-- "crates/ruststream-fred/examples/fred_reclaim.rs:reclaim"
```

## 批次 { #batches }

切片参数把处理器变成批量处理器，批次大小由挂载点写明：

```rust
--8<-- "crates/ruststream-fred/examples/fred_seek.rs:batch-mount"
```

`batch(n)` 在批量处理器上是必填的，在单条消息的处理器上则不接受。在流上它成为取这一批的
`XREADGROUP`（或 `XAUTOCLAIM`）的 `COUNT`：服务器最多发这么多条目，处理器看到的正是一次读取返回的
内容。

Redis 自己的读取选项在大小之后接着链下去，来自各形式 prelude 里的 `RedisSubscribeExt`。`block(..)`
设定一次读取等待条目的时长，在那里写的值覆盖描述符里的值。

列表和 Pub/Sub 一次弹出一个条目，因此它们的订阅者在客户端侧攒批，并遵守同样的大小。

## 传输自带的投递字段 { #native-delivery-fields }

Redis 带有载荷里没有的元数据，处理器按键从自己的类型化上下文里读取。处理器把 `StreamContext` 写作
自己的上下文类型，或者用 `Ctx<K>` 参数绑定其中一个键；批量的函数体写 `StreamBatchContext`。这个
传输没有的键无法通过编译。

| 键 | 值 | 位置 |
| --- | --- | --- |
| `keys::EntryId` | `EntryId`，本次投递读到的、已解析的 `<milliseconds>-<sequence>` 标识 | 投递 |
| `keys::Position` | `RedisGroupPosition`，重新投递该条目的游标 | 投递 |
| `keys::ConsumerGroup` | 订阅读取时所经过的组 | 投递和批次 |
| `keys::SeekHandle` | `RedisGroupSeeker`，该组的重新定位句柄 | 投递和批次 |

一个批次跨越多次投递，因此 `StreamBatchContext` 只带属于订阅的东西。条目标识和位置属于单次投递，
从批次自己的元素上读取；在批次上下文上要这样的键无法通过编译。

回收这条路径把投递次数和空闲时间报在 `DELIVERY_COUNT_HEADER` 和 `IDLE_MS_HEADER` 两个消息头里，
每种传输读它们的方式都一样。Pub/Sub 有自己的 `PubSubContext`（匹配到的频道，以及它是不是经由模式
而来）；列表除了载荷和消息头什么都不带，因此它的上下文是 `()`。

## 重新定位一个组 { #repositioning-a-group }

组可以沿历史往回移，也可以往前跳过它应当跳过的条目。`StreamStart` 选定组创建时从哪里开始；移动
一个已经存在的组则是 `Seekable` 能力，流实现了它。

**定位作用于整个组。** Redis 每个消费者组只有一个游标，因此一次定位会移动该组的每个消费者，不只是
提出请求的那个订阅。类型的名字写明了这个范围：`RedisGroupPosition` 和 `RedisGroupSeeker`。

三个位置，各由一个构造函数给出：

| 构造函数 | 组从哪里继续 |
| --- | --- |
| `RedisGroupPosition::beginning()` | 流中仍然保留的最老的条目 |
| `RedisGroupPosition::end()` | 流的末尾：只有之后追加的条目 |
| `RedisGroupPosition::after(id)` | `id` 之后的那个条目（游标不含端点，和 `XGROUP SETID` 一样） |

`start_at(..)` 子句在订阅第一次投递之前定位它，每次启动都做一次：

```rust
--8<-- "crates/ruststream-fred/examples/fred_seek.rs:start-at"
```

服务运行期间，处理器通过自己上下文里的句柄定位：`StreamContext` 在 `keys::SeekHandle` 键下带着该组
的定位句柄，用 `Ctx` 参数绑定。

```rust
--8<-- "crates/ruststream-fred/examples/fred_seek.rs:seek-param"
```

每次投递都报出自己的位置（`Positioned::position`，同一个值也在 `keys::Position` 键下）。定位到这个
位置会重新投递这条消息，随后是它之后的条目：游标不含端点，因此标识已经替你减过一位。

批量的函数体定位的是同一个组。定位句柄属于订阅，因此它在 `StreamBatchContext` 上，键也一样；批次
所响应的那个条目来自批次自己的元素：

```rust
--8<-- "crates/ruststream-fred/examples/fred_seek.rs:batch"
```

定位不影响这些：

- **待处理条目列表。** 已经投递出去、还没 ack 的条目，无论游标往哪边移动都仍然待处理，并且仍然可
  以经由回收这条路径拿到。
- **已排期的延迟重试。** 已经躺在 ZSET 延迟队列里的副本以到期时间为分值，因此时间一到就追加回流里，
  与组读到哪里无关。
- **投递次数。** 重放的条目会再投递一次，它自带的投递次数因此增长；带 `max_deliveries` 的回收订阅
  于是把重放算进上限，而框架的重试次数消息头只在真正的 `nack` 上才增加。

定位一返回，游标就已经改变；阻塞在 `XREADGROUP` 里的订阅在下一次读取时看到它，最多差一个 `block`
间隔。旧游标选中的条目直接丢弃，不再投递。

## 确认 { #acknowledgement }

结算遵循重新发布的重试模型：

- `ack` -> `XACK`（从待处理列表中移除）。
- `nack(requeue = true)` -> 往同一个流里追加一份副本，然后对原件 `XACK`。副本由普通的 `new` 消费者
  重新处理。这是至少一次：两步之间崩溃会留下一份重复。
- `nack(requeue = false)` -> `XACK` 以丢弃。

## 延迟重试 { #delayed-retry }

`HandlerOutcome::retry_after(delay)` 请求一次延迟的重新投递，用来给暂时性的错误退避。Redis Streams
没有逐条消息的延迟，因此一个订阅通过两种方式之一拿到延迟。

运行时自己的方式是：延迟到期时，把这条消息的一份副本发布回流里：

```rust
--8<-- "crates/ruststream-fred/examples/fred_delayed_retry.rs:deferred"
```

这份副本经由你在挂载点用 `out_retry` 写明的发布者发出。不写它，延迟就被丢掉，消息立刻重新入队。
副本在整个延迟窗口里是至多一次：定时器触发前崩溃就会丢掉它。这个位置是普通的 `Out` 槽位，因此它
后面的 `.transform(..)` 作用在副本上，而副本的重试次数消息头比原件高一。

ZSET 延迟队列是持久的方式：排期的条目存在 Redis 里，因此重新投递能挺过重启。它默认关闭，ZSET 的
键由你写明：

```rust
--8<-- "crates/ruststream-fred/examples/fred_delayed_retry.rs:handler"
```

延迟的投递用 `ZADD` 以到期时间写进那个 ZSET，重试次数消息头随之加一，原件用 `XACK` 确认。订阅一边
读取一边扫这个 ZSET，用 `XADD` 把到期的条目原样放回流里。每次读取扫一遍，因此粒度就是 `block`
间隔，而一遍最多搬回 128 个到期条目。ZSET 键上的 TTL 清理无人再管的队列，它必须比最长的排期延迟
还长，否则条目会在触发之前就消失。分值是墙上时钟的纪元毫秒，因此要让各机器的时钟保持同步（NTP）。

两个订阅并排挂载，只有没有队列的那个写明发布者：

```rust
--8<-- "crates/ruststream-fred/examples/fred_delayed_retry.rs:app"
```

## 分区键 { #partition-keys }

`workers(n, by_key)` 让一个订阅在多个 worker 上运行，并保持每个键内部的顺序：分区键相同的投递进入
同一个分区。`partition_key` 是发布上的一个步骤，因此它给单条消息设键：

<!-- inline-rust: two-publish fragment isolating the step; the compiled call sites are the crate's `partition_key` doctests, which need a connected broker and so cannot double as a snippet source here -->
```rust
use ruststream_fred::stream::prelude::*;
use serde::Serialize;

#[derive(Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

publisher.message(&Order { id: 7 }).partition_key("tenant-a").publish().await?;
publisher.message(&Order { id: 8 }).partition_key("tenant-a").publish().await?;
```

这个步骤放在消息之后的任何位置，它不占用自己的位置，因此能和已声明的消息头契约组合：

<!-- inline-rust: isolates the contract-plus-key chain; the compiled form is the `partition_key_step_composes_with_a_header_contract` test, whose broker setup would bury the four lines that matter -->
```rust
#[derive(Serialize, Outgoing)]
#[outgoing(name = "orders.keyed", headers = OrderMeta)]
struct KeyedOrder {
    id: u64,
}

#[derive(Serialize)]
struct OrderMeta {
    region: String,
}

publisher
    .message(&KeyedOrder { id: 7 })
    .with_headers(&OrderMeta { region: "eu".into() })
    .partition_key("tenant-a")
    .publish()
    .await?;
```

处理器给自己的发布设键也是同样的写法。这个步骤属于本 Broker，因此这样的函数体导入本 crate 的
prelude，并在槽位的约束里写出设置类型：

<!-- inline-rust: the signature is the subject here; the compiled form is the `the_step_sets_the_key_the_delivery_reports` test, whose mount and assertions would bury it -->
```rust
use ruststream_fred::stream::prelude::*;

#[derive(OutSlot)]
#[publishes(Order)]
struct Ledger;

#[subscriber(RedisStream::new("orders.in").group("workers"))]
async fn forward(
    order: &Order,
    Out(ledger): Out<impl Publisher<Options = RedisPublishOptions>, Ledger>,
) -> HandlerOutcome {
    if ledger
        .message(order)
        .to("orders.keyed")
        .partition_key("tenant-a")
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}
```

Redis 自己没有分区，因此发布者把定下来的键写进 `redis-partition-key` 消息头，消费侧也从那里读它。
在调用处写这个消息头，是能跨 Broker 通用的写法；步骤会为自己这条消息覆盖它，而没有用步骤的发布不
动消息头。消息头的名字以 `PARTITION_KEY_HEADER` 公开，三种传输都接受这个步骤。

## 能力 { #capabilities }

框架的可选能力 trait，以及本 Broker 实现了其中的哪些。备注里写明列表和 Pub/Sub 与 Streams 的差别。

| 能力 | 原生 | 备注 |
| --- | --- | --- |
| `Subscribe` | 是 | 按流的键经由消费者组订阅（裸字符串这种形式需要 `default_group`）。列表和 Pub/Sub 经由各自的描述符订阅。 |
| `BatchSubscriber` | 是，三者都有 | 在 Streams 上是原生的：挂载点的 `batch(n)` 就是取这一批的 `XREADGROUP` / `XAUTOCLAIM` 的 `COUNT`，一个批次就是一次非空读取，绝不为空。列表和 Pub/Sub 一次弹出一个条目，因此它们的订阅者在客户端侧攒批，并遵守同样的大小。见[批次](#batches)。 |
| `TransactionalPublisher` | 是（Streams，standalone 和 sentinel） | 流的发布者在句柄上缓冲，并把缓冲作为一个 `MULTI` / `EXEC` 提交。集群上的发布者拒绝它，因为一个 `MULTI` 块不能跨哈希槽。列表和 Pub/Sub 的发布者没有事务。见[事务](transactions.md)。 |
| `OwnedTransactions` | 是（Streams，standalone 和 sentinel） | `publisher.transaction()` 返回一个自己持有缓冲的值，因此一个句柄上可以同时开任意多个；在集群上出于同样的原因拒绝。 |
| `RequestReply` | 否 | Redis 没有请求-应答原语：传输过程中没有任何东西存放应答地址，也没有任何东西把应答和请求对应起来。 |
| `Partitioned` | 是 | 三种传输都从 `redis-partition-key` 消息头读取键，供运行时的 `workers(n, by_key)` 分区使用。发送方用 [`partition_key`](#partition-keys) 步骤设定它。 |
| `Seekable` + `Positioned` | 是（Streams） | 组的游标用 `XGROUP SETID` 移动，投递则报出能把自己重新投递一次的位置。处理器经由 `keys::SeekHandle` 上下文键拿到句柄；见[重新定位一个组](#repositioning-a-group)。列表读取即销毁，Pub/Sub 不保留历史，因此两者都没有实现它。 |
| `DescribeServer` | 是 | 报出客户端拨号所用的主机和端口（集群和 sentinel 上是第一个种子节点）。URL 里的凭据、数据库编号和查询参数不会进入生成的文档。用 `RedisBroker::from_pool` 建的 Broker 根本不报主机：地址在连接池自己的配置里。 |
