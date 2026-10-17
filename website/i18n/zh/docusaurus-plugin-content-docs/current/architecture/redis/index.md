---
title: 传输方式
sidebar_position: 0
description: "Redis 用三种方式传递消息：流为消费者组保留条目，列表把每个条目交给一个消费者，频道什么也不保留。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "生产者写入流、列表和频道，每种传输方式把消息交给读取者的方式各不相同",
  "text": "以文字阅读本图",
  "streamProducer": "XADD",
  "listProducer": "LPUSH",
  "channelProducer": "PUBLISH",
  "stream": ["流", "保留每个条目"],
  "list": ["列表", "直到被弹出"],
  "channel": ["频道", "什么也不保留"],
  "groupA": ["组", "billing"],
  "groupB": ["组", "audit"],
  "workers": ["消费者", "轮流读取"],
  "subA": ["客户端", "A"],
  "subB": ["客户端", "B"],
  "late": ["之后才加入：", "永远收不到"],
  "keeps": ["每个组都读取", "每个条目"],
  "once": ["每个条目只交给", "一个消费者"]
};

# 传输方式

Redis 用三种方式传递消息：流为消费者组保留条目，列表把每个条目交给一个消费者，频道什么也不保留。

<Diagram name="transports" words={words} />

流是只追加的日志。`XADD` 以服务器分配的 id 追加一个条目，条目一直保留到流被裁剪为止。消费者组用自己的游标读取流，因此两个组各自收到每个条目。在同一个组内，每个条目只交给一个消费者。[消费者组](groups.md)一页用图说明：组如何跟踪它投递过的条目。

列表是一个队列。生产者用 `LPUSH` 推入，消费者从另一端弹出，弹出的条目就从 Redis 中消失了。[队列](queues.md)一页用图说明：可靠列表如何把条目一直保留到处理器结算它为止。

频道不存储任何东西。`PUBLISH` 把消息推送给此刻已订阅的客户端，之后才订阅的客户端永远收不到它。[广播](broadcast.md)一页用图说明了这一点。

服务通过各自的描述符读取每种传输方式：[`RedisStream`](api:ruststream_fred::stream::RedisStream)、[`RedisList`](api:ruststream_fred::list::RedisList) 和 [`RedisPubSub`](api:ruststream_fred::pubsub::RedisPubSub)。三者的处理器都是同一个 `#[subscriber]` 函数。确认的含义各不相同，[结算](../settlement.md)一页用图说明了这一点。
