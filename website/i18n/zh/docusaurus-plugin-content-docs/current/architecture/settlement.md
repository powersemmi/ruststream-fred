---
title: 结算
sidebar_position: 5
description: "ack、retry、drop 和 retry_after 在 Redis 流、可靠列表、简单列表和 Pub/Sub 上变成什么。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const table = {
  "title": "每种结果在每种形式上发送的命令：流、认领式流、可靠列表，以及没有确认的形式",
  "text": "以文字阅读本图",
  "outcome": "结果",
  "stream": "流",
  "claiming": "认领式",
  "reliable": "可靠列表",
  "none": "无确认",
  "ack": "ack",
  "retry": "retry",
  "drop": "drop",
  "ack-stream": "XACK",
  "ack-claiming": "XACK",
  "ack-reliable": "LREM",
  "ack-none": "不支持",
  "retry-stream": ["XADD 副本", "再 XACK"],
  "retry-claiming": ["保持", "待处理"],
  "retry-reliable": ["LPUSH", "再 LREM"],
  "retry-none": "不支持",
  "drop-stream": "XACK",
  "drop-claiming": "XACK",
  "drop-reliable": "LREM",
  "drop-none": "不支持"
};

export const delays = {
  "title": "流订阅提供 retry_after 的三种方式：进程中的副本、Redis 有序集合中的副本，或让条目保持待处理",
  "text": "以文字阅读本图",
  "copyLane": "运行时副本",
  "zsetLane": "延迟队列",
  "claimingLane": "认领式",
  "outcome": "对流条目返回 retry_after(delay)",
  "copyAck": ["XACK", "原条目"],
  "held": ["副本保存在", "进程中：", "崩溃即丢失"],
  "republish": ["计时器触发时", "XADD 副本"],
  "zadd": ["ZADD 副本，", "再 XACK"],
  "zset": ["在 Redis 中，", "以到期时间", "为分值"],
  "sweep": ["由读取清扫：", "XADD 回到流"],
  "pending": ["留在待处理", "条目列表中"],
  "claimed": ["min_idle 之后", "被认领回来"],
  "timer": "延迟",
  "due": "到期",
  "idle": "空闲"
};

# 结算

`ack`、`retry`、`drop` 和 `retry_after` 在 Redis 流、可靠列表、简单列表和 Pub/Sub 上变成什么。

<Diagram name="settlement" words={table} />

运行时用处理器返回的[结果](../components/acknowledgement/index.md)结算每次投递，本 crate 把结果映射到该形式自己的确认。流在订阅的组里用 `XACK` 确认。可靠列表用 `LREM` 把条目从它的处理中列表移除。简单列表和 Pub/Sub 没有可确认的东西，在那里结算会返回 `AckError::Unsupported`。结算命令走连接池，而不是订阅的读取客户端。

流上的重试把条目的副本追加到同一个流，然后确认原条目。两步之间崩溃，留下的是重复，而不是丢失。[认领式](api:ruststream_fred::stream::RedisStream::claiming)订阅则通过待处理条目列表重试。它什么也不追加，也不确认；条目空闲达到 `min_idle` 之后，下一次读取把它认领回来，id 不变，投递次数加一。可靠列表先把条目推回队列，再把它从处理中列表移除。

## 带延迟的重试 {/* #retries-with-a-delay */}

<Diagram name="delay" words={delays} />

Redis 没有针对单个条目的延迟，因此流订阅用三种方式之一提供 [`retry_after`](api:ruststream::runtime::HandlerOutcome::retry_after)。默认情况下，运行时确认原条目并持有一个副本，延迟到期时把副本发布回流。副本的重试计数加一。计时器触发前崩溃，副本就会丢失。

[`delayed_retry`](api:ruststream_fred::stream::RedisStream::delayed_retry) 配合 [`DelayedRetry::DurableZset`](api:ruststream_fred::DelayedRetry::DurableZset) 可以[把副本保存在 Redis 中](../components/acknowledgement/delayed-retry.md)。副本先加入一个以到期时间为分值的有序集合，然后原条目才被确认。每次读取先用 `XADD` 把到期的副本移回流上，每次最多 128 个，因此延迟的误差不超过一个 `block` 间隔。分值是挂钟时间的毫秒数，所以共享同一队列的各个服务，时钟应当一致。

认领式订阅让条目保持待处理，`min_idle` 过去之后，下一次读取把它认领回来。短于 `min_idle` 的延迟也要等满 `min_idle`。

列表订阅和 Pub/Sub 订阅使用运行时的副本，副本回到列表或频道。模式订阅的副本发往何处，在它的挂载点指定。

`max_attempts` 上限用两个计数之一统计尝试次数，用哪一个由订阅形式决定。认领模式和回收模式使用服务器在待处理条目列表中记录的投递次数。其他所有形式使用副本携带的重试计数消息头，延迟队列每一轮都会把它加一。
