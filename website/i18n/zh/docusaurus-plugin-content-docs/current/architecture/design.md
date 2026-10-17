---
title: 设计
sidebar_position: 2
description: "ruststream-fred 的位置：在 RustStream 运行时与 fred 客户端之间，每个订阅一个自己的客户端，共用一个连接池。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "自上而下依次是服务、RustStream 运行时、本 crate、fred 客户端和 Redis",
  "text": "以文字阅读本图",
  "service": "你的服务",
  "handlers": ["#[subscriber]", "处理器"],
  "app": ["RustStream::new(..)", ".with_broker(..)"],
  "core": "RustStream 核心",
  "runtime": "运行时：分发、解码、结算",
  "crate": "ruststream-fred",
  "subscribers": ["订阅者", "读取流、列表", "和频道"],
  "publishers": ["发布者", "同样的", "三种形式"],
  "descriptors": ["RedisStream", "RedisList", "RedisPubSub"],
  "window": ["流水线窗口", "由 .pipeline() 开启"],
  "broker": ["RedisBroker", "拓扑、连接池、凭据"],
  "client": "fred",
  "reader": ["每个订阅", "一个自己的客户端"],
  "pool": ["连接池", "4 个连接"],
  "redis": "Redis 或 Valkey：单机、集群或 Sentinel",
  "mounted": "挂载到",
  "polls": "轮询",
  "publishes": "发布"
};

# 设计

`ruststream-fred` 所处的位置：在 RustStream 运行时与 fred 客户端之间，每个订阅一个自己的客户端，共用一个连接池。

<Diagram name="design" words={words} />

本 crate 在 [fred](https://docs.rs/fred/10.1.0/fred/) 之上实现核心的 Broker trait。它上面的运行时对每个 Broker 都相同。Redis 特有的一切都在本 crate 里：三种传输方式的描述符、确认在每种方式上的含义、重试副本发往何处，以及流水线窗口。

描述符在启动时针对已连接的 Broker 解析一次，产出一个订阅者。订阅者是一个投递流，由运行时轮询。运行时解码每次投递，运行处理器，并用处理器的结果结算这次投递。发布策略与已连接的 Broker 配对，构造出发布者。

每个订阅在自己的客户端上读取，因此阻塞的读取永远不会拖住发布。发布、结算以及处理器发送的命令共用连接池。各个客户端何时打开和关闭，见[生命周期](lifecycle.md)；一个条目的路径，见[路径](path.md)。

流条目把载荷放在名为 `_payload` 的字段里，把每个消息头放在字段 `h:<name>` 里，因此 `XADD` 和 `XREADGROUP` 都会保留两者。列表条目和频道消息把消息头放在包裹载荷的帧里：二进制帧，或者在描述符指定了编解码器时，使用编解码器信封。
