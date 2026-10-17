---
title: 广播
sidebar_position: 3
description: "Redis Pub/Sub 把消息推送给发布时已连接的订阅，不为其他任何人保留。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "Pub/Sub：一次发布到达此刻已连接的订阅，各自经过其客户端的缓冲区",
  "text": "以文字阅读本图",
  "publisher": ["发布者", "PUBLISH news"],
  "channel": ["频道 news", "什么也不保留"],
  "subA": ["订阅 A", "已连接"],
  "subB": ["订阅 B", "已连接"],
  "late": ["之后才", "订阅：", "永远收不到"],
  "buffer": ["客户端缓冲区", "默认 32 条"],
  "handler": "处理器",
  "overflow": ["处理器跟不上缓冲区时，", "最旧的消息会丢失"],
  "publish": "发布"
};

# 广播

Redis Pub/Sub 把消息推送给发布时已连接的订阅，不为其他任何人保留。

<Diagram name="broadcast" words={words} />

频道是一个名字，不是键：Redis 不在它下面存储任何东西。`PUBLISH` 把消息推送给此刻订阅了该频道的每个客户端。Redis 称之为至多一次投递：没有订阅或断开了连接的客户端，永远收不到这条消息（[Pub/Sub](https://redis.io/docs/latest/develop/pubsub/)）。

每个 [`RedisPubSub`](api:ruststream_fred::pubsub::RedisPubSub) 订阅都打开一个自己的客户端，并用它订阅。经典订阅要等同一连接上的一次 `PING` 得到应答后才返回，因此订阅打开之后发布的消息都会到达它。[`RedisPubSubPattern`](api:ruststream_fred::pubsub::RedisPubSubPattern) 用 `PSUBSCRIBE` 订阅一个 glob 模式，接收与之匹配的每个频道。

消息在客户端的缓冲区里等待，直到处理器取走它。缓冲区容纳 32 条消息，这是 `fred` 的默认值，[`buffer`](api:ruststream_fred::pubsub::RedisPubSub::buffer) 可以设置别的大小。落后的订阅会丢失最旧的消息，并记录一条警告，写明频道和丢失的条数。

经典 Pub/Sub 把每条消息发送到集群的每个节点。分片 Pub/Sub 自 Redis 7.0 起提供，命令是 `SSUBSCRIBE` 和 `SPUBLISH`：它把频道分配给一个哈希槽，消息只留在该分片之内。[`PubSubMode`](api:ruststream_fred::pubsub::PubSubMode) 选择其中一种，一种模式下的订阅收不到另一种模式下发布的消息。

Pub/Sub 投递没有什么需要结算：`ack` 和 `nack` 返回 `AckError::Unsupported`。
