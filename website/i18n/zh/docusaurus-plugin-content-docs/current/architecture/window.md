---
title: 窗口
sidebar_position: 6
description: "流水线化的订阅在窗口中结算：一次读取的各个投递排入各自的副作用，一次刷出把它们和结算一起发送。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "流水线化流订阅的一次读取：每次投递排入一条命令，结果保留或丢弃它，一次刷出发送剩下的命令",
  "text": "以文字阅读本图",
  "entry": "条目 {id}",
  "queues": "排入一条 INCR",
  "kept": ["ack：", "保留段"],
  "dropped": ["drop：", "丢弃段"],
  "flush": "一次刷出，一次往返",
  "segment": "{id} 的 INCR",
  "settleOnly": "只结算",
  "atomic": ["在 .atomic() 下，每个段是 MULTI、它的命令、", "该投递自己的结算，最后是 EXEC"]
};

# 窗口

[流水线化](../components/pipelining/index.md)的订阅在窗口中结算：一次读取的各个投递排入各自的副作用，一次刷出把它们和结算一起发送。

<Diagram name="window" words={words} />

在挂载点调用 [`.pipeline()`](api:ruststream_fred::pipeline::RedisPipelineSteps::pipeline)，就会在订阅上打开一个窗口。一次读取的每个投递在窗口里占一个槽。投递的处理器通过 `Ctx<keys::Pipeline>` 排入的命令，进入这个槽的段。段是一个 `fred` 流水线，在第一条命令排入时打开。处理器看不到命令的应答。

结果决定段的去留：`ack` 保留它，`drop`、`retry` 和 `retry_after` 丢弃它。每种结果发送的结算，与没有窗口时该形式发送的结算相同。未结算就被丢弃的投递不发送结算：它的段被丢弃，它的条目保持待处理。

在三种情况下窗口会刷出：已结算的投递数达到这次读取的 `COUNT`，没有未完成的投递，或者订阅停止。刷出先按提交顺序发送保留下来的段，再在一个流水线里发送这次读取的结算，使用连接池的一个连接。流的结算合并成一条 `XACK`，可靠列表的结算是每个条目一条 `LREM`。在负载下，每次读取只需一次往返。

[`.atomic()`](api:ruststream_fred::pipeline::RedisPipelineSteps::atomic) 把每个段包进 `MULTI` 和 `EXEC`，并把该投递自己的结算放在里面。于是处理器的副作用和它的确认同时生效。Redis 没有回滚：`EXEC` 内某条命令失败时，其他命令仍然会执行（[事务](https://redis.io/docs/latest/develop/using-commands/transactions/)）。在集群上，段中的每个键都必须与订阅的键位于同一个槽。

刷出失败时，错误在投递流上报告一次。在需要结算的形式上，结算失败的条目保持待处理，之后会被再次投递。
