---
title: 生命周期
sidebar_position: 4
description: "RedisBroker 只记录配置，connect 打开连接池，每个订阅打开一个自己的客户端，关闭时解除阻塞并关闭它们。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "Broker 的三个状态作为类型，以及每次转换在 Redis 上做什么",
  "text": "以文字阅读本图",
  "states": "Broker",
  "redis": "在 Redis 上",
  "unconnected": ["RedisBroker::", "standalone(url)"],
  "connected": ["ConnectedRedisBroker"],
  "closed": ["ClosedRedisBroker", "connections_closed"],
  "config": ["只记录配置，", "无 I/O"],
  "pool": ["4 个连接的连接池：", "发布、结算"],
  "subscriptions": ["每个订阅：", "创建它的组，", "一个自己的客户端"],
  "unblock": ["订阅被丢弃：", "CLIENT UNBLOCK、QUIT"],
  "quit": ["等待每个读取客户端", "关闭，向连接池 QUIT"],
  "afterShutdown": ["比连接活得更久的发布者或投递", "立即返回 RedisError::ShutDown"],
  "connect": "connect(self)",
  "shutdown": "shutdown(self)"
};

# 生命周期

`RedisBroker` 只记录配置，connect 打开连接池，每个订阅打开一个自己的客户端，关闭时解除阻塞并关闭它们。

<Diagram name="lifecycle" words={words} />

[`RedisBroker::standalone`](api:ruststream_fred::RedisBroker::standalone) 记录 URL，不做任何 I/O，`cluster` 和 `sentinel` 也一样，因此服务的构建函数保持同步。运行时在应用启动时连接 Broker。connect 把拓扑、凭据和 TLS 设置转换成 `fred` 的配置，并打开一个连接池，连接数由 [`pool`](api:ruststream_fred::RedisBroker::pool) 设置，默认四个。服务器不可达时，启动返回错误。用 [`from_pool`](api:ruststream_fred::RedisBroker::from_pool) 构建的 Broker 接管交给它的连接池，并在这里连接它，除非服务已经连接过。

发布、结算以及处理器发送的命令都走连接池。每个订阅在订阅时打开一个自己的客户端。流订阅和列表订阅在这个客户端上读取。原因是阻塞读取（带 `BLOCK` 的 `XREADGROUP`、`BLMOVE`、`BRPOP`）在返回之前一直占着它的连接，排在它后面的发布就得等完整个阻塞间隔。Pub/Sub 订阅在这个客户端上接收消息。流订阅在返回之前创建它的消费者组。

订阅被丢弃时，它的客户端在 `fred` 的旁路连接上发送 `CLIENT UNBLOCK`，结束仍阻塞在服务器上的读取，然后发送 `QUIT`。阻塞的弹出如果继续运行，就会替一个已经没人读取的订阅取走下一个条目。

服务器断开任何一个连接后，该连接都会重新连接，退避时间从 100 毫秒增长到 10 秒。在此期间发出的命令会等待重连完成。

[关闭](../components/broker/shutdown.md)把连接标记为已关闭，等待每个读取客户端关闭，然后向连接池发送 `QUIT`。[`ClosedRedisBroker`](api:ruststream_fred::ClosedRedisBroker) 报告它关闭了多少个连接池连接。比连接活得更久的发布者、seeker 或投递，在发送之前检查这个标记，并立即返回 [`RedisError::ShutDown`](api:ruststream_fred::RedisError::ShutDown)。
