---
title: 路径
sidebar_position: 3
description: "一个流条目在 RustStream 服务中的路径：追加、组读取、投递、处理器、应答和 XACK。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "一个需要应答的流条目的路径：从追加到确认",
  "text": "以文字阅读本图",
  "append": ["XADD", "一个 _payload 字段和若干 h: 字段"],
  "read": ["XREADGROUP", "最多 64 个条目，在读取客户端上"],
  "entry": ["RedisMessage", "字段变成载荷和消息头"],
  "dispatch": ["分发", "运行时的循环或它的工作任务"],
  "decode": ["解码", "把载荷解码成处理器的类型"],
  "body": ["订阅者的处理器", "#[subscriber] 下的 async fn"],
  "reply": ["应答", "在连接池的连接上 XADD"],
  "settle": ["结算", "由结果决定"],
  "ack": ["XACK", "在连接池的连接上"]
};

# 路径

一个流条目在 RustStream 服务中的路径：追加、组读取、投递、处理器、应答和 `XACK`。

<Diagram name="path" words={words} />

一次读取在一次往返中返回最多 64 个条目，订阅把它们逐个交出，交完之后再读取。批处理器则按自己的批大小读取条目。流里没有新内容时，读取会阻塞订阅的 `block` 间隔，默认五秒。

每个条目变成一个 [`RedisMessage`](api:ruststream_fred::RedisMessage)：`_payload` 字段是载荷，每个 `h:` 字段是一个消息头。运行时解码载荷并调用处理器。

应答经过编码，在连接池的一个连接上用 `XADD` 发布，之后投递才结算。列表读取和 Pub/Sub 消息从投递开始走同样的路，只是各按自己形式的方式结算，[结算](settlement.md)一页用图说明了这一点。
