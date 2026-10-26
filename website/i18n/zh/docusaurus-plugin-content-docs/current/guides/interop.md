---
sidebar_position: 13
description: "与其他语言的 Redis 客户端交换消息：流条目的布局、列表和频道的帧，以及 JSON 信封。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 与其他客户端互通

本指南说明 RustStream 服务在 Redis 中读写的内容，让其他语言的生产者和消费者能共享它的流、列表和频道。

示例是一个订单服务。
用另一种语言编写的商店向流追加订单，用另一种语言编写的邮件程序从列表弹出邮件。

## 读取流条目 {/* #reading-a-stream-entry */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/interop.rs#read"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/interop.rs#read"
```

</TabItem>
</Tabs>

Redis 流条目是一组字段。
RustStream 服务把消息体放在字段 `_payload` 中，把每个消息头放在名为 `h:` 加消息头名字的字段中。
任何语言的生产者都可以这样写入一个服务能读取的订单：

```bash
redis-cli XADD orders '*' _payload '{"id":1,"email":"ann@example.com"}' h:trace-id 4bf92f35
```

消息体就是编解码器解码的内容：使用默认编解码器时是 JSON，所以生产者把订单写成 JSON。
消息头字段是可选的；只有 `_payload` 的条目到达时不带消息头。
其他名字的字段会被忽略，所以生产者可以加入自己的字段。
没有 `_payload` 的条目由其他程序写入，它的字段作为一个 JSON 对象成为消息体。
有效 UTF-8 的值写成字符串，其他字节写成数字数组；`h:` 字段仍然是消息头：

```bash
redis-cli XADD orders '*' id 1 email ann@example.com
# 消息体：{"email":"ann@example.com","id":"1"}
```

其他语言的消费者用同样的方式读取服务写入的条目：`_payload` 保存消息体，`h:` 字段保存消息头。

## 写入列表值 {/* #writing-a-list-value */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/interop.rs#write"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/interop.rs#write"
```

</TabItem>
</Tabs>

列表条目和 Pub/Sub 消息都是单个值，所以消息头放在包住消息体的帧中。
没有消息头的消息直接写成消息体。
默认的帧是二进制的，以一个名为 `ruststream-frame`、值为空的标记消息头开头：

| 字节 | 含义 |
| --- | --- |
| 4 | 消息头的个数加上标记的一个，大端序 `u32` |
| 4，`ruststream-frame`，4 | 标记：名字长度 16、名字和值长度 0 |
| 4，然后是名字 | 每个消息头：名字的长度和名字 |
| 4，然后是值 | 值的长度和值 |
| 剩余部分 | 消息体 |

没有消息头的消息体如果在帧放标记的位置带有标记，就写在帧中，所以读回时不变。

发布策略上的[编解码器](../components/subscriptions/framing.md)改为用该编解码器写入一个信封。
使用 `JsonCodec` 时，邮件程序弹出的是一个 JSON 对象：

```bash
redis-cli RPOP emails
# {"headers":{},"payload":"{\"to\":\"ann@example.com\",\"subject\":\"Order 1 received\"}"}
```

`payload` 是消息体，这里是 JSON 文本形式的邮件，所以邮件程序先解析信封，再解析其中的 `payload`。
有效 UTF-8 的消息体或消息头写成字符串，其他字节写成数字数组。
在 RedisInsight 这样的工具中，同样的信封显示为可读的值。

## 读取列表值或频道消息 {/* #reading-a-list-value-or-a-channel-message */}

订阅读取其发布者写入的帧：[`RedisList::codec`](api:ruststream_fred::list::RedisList::codec) 和 [`RedisPubSub::codec`](api:ruststream_fred::pubsub::RedisPubSub::codec) 设置信封的编解码器，没有设置时订阅读取二进制帧。
设置了编解码器的订阅也能读取二进制帧，服务切换编解码器时会留下这样的帧。
信封只按确切的形式读取：带有 `headers` 和 `payload`、没有其他字段的对象。

既不是二进制帧也不是信封的值，例如另一个客户端以纯 JSON 推入或发布的值，无论前几个字节是什么，都会作为不带消息头的消息体到达：

```bash
redis-cli LPUSH jobs '{"id":7}'
redis-cli PUBLISH prices '{"sku":"tea","cents":450}'
```

因此，其他语言的生产者写入纯值即可，需要消息头时再写入 JSON 信封。

0.7.1 及更早版本写入的二进制帧没有标记，当前的订阅把它连同帧的字节一起读作消息体。
因此共用一个列表或频道的所有服务一起升级，并先读完列表中的旧值。
更早版本的订阅能读取新帧，并把标记看作多出的一个消息头。

## 测试 {/* #testing */}

示例文件中的测试在进程内运行：订单按商店写入的样子到达，`emails` 上的值逐字节就是 JSON 信封。
live 测试像另一个程序那样，用第二个 `fred` 客户端从服务器读取这两者：`orders` 最新的条目在 `_payload` 下保存订单，`emails` 最新的值就是信封。
