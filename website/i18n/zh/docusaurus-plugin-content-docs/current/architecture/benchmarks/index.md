---
title: 基准测试
description: "ruststream-fred 在每条消息上的开销：与裸 fred 客户端对照的吞吐量，以及它自身代码的指令数和内存分配次数。"
---

import {BenchmarkEnvironment, BenchmarkResults, MovedSections} from '@site/src/components/Benchmarks';

export const labels = {"loading": "正在读取结果...", "scenario": "场景", "raw": "裸客户端", "adapter": "Broker crate", "framework": "RustStream", "overhead": "额外开销", "against": "（比客户端 {percent}）", "indistinguishable": "无法区分", "brokerBound": "受 Broker 限制", "measured": "测量于", "details": "这些数字是怎样测得的", "pending": "尚未公布结果：{brokers}。", "instructions": "指令数", "allocations": "内存分配", "cold": "冷启动", "table": "以表格显示数字", "axis": "各循环比裸客户端最佳一轮低多少", "rounds": "各轮，从最佳到最慢", "best": "最佳一轮", "median": "中位一轮", "noise": "轮次的波动范围", "comparison": "与裸客户端对照", "code": "代码的开销", "broker": "Broker", "plugin": "{plugin} 插件", "pluginRaw": "不用插件", "pluginAdapter": "手写的插件", "pluginFramework": "crate 自带的插件", "pluginAxis": "各循环比不用插件的应用最佳一轮低多少", "pluginAgainst": "（比不用插件 {percent}）", "pluginOverhead": "比手写插件", "pluginOverHand": "（比手写插件 {percent}）"};

export const machine = {"cpu": "处理器", "architecture": "架构", "cpu_frequency": "频率", "cores": "核心数", "memory": "内存", "memory_speed": "内存速度", "os": "操作系统", "broker": "Broker", "rustc": "编译器", "profile": "构建配置", "features": "特性", "rustflags": "RUSTFLAGS", "round_trip": "往返时间", "valgrind": "Valgrind"};

# 基准测试

`ruststream-fred` 在每条消息上的开销：与裸 fred 客户端对照的吞吐量，以及它自身代码的指令数和内存分配次数。

## 与裸客户端对照 {/* #against-a-raw-client */}

第一张图在真实的 Redis 服务器上把每个场景用三种方式运行。`裸客户端` 是在 `fred` 上手写的循环。`Broker crate` 是在本 crate 上写的同一个循环：Broker、订阅、投递流和 ack，上面没有运行时。`RustStream` 是用户写的服务：应用里的一个 `#[subscriber]` 处理器。第一项差值是本 crate 比它所封装的客户端多付出的开销，第二项是运行时在其上增加的开销。

每个循环画成一条横条，从最佳一轮延伸到最慢的一轮，圆点标出最佳一轮，缺口标出中位一轮。坐标轴的零点是裸客户端的最佳一轮，圆点在零点右侧表示更慢。空心圆点表示该循环与裸客户端的差异小于各轮的波动范围。图表下方的表格列出最佳一轮，括号里是中位一轮。

测量的形式有三种，每种传输方式一种：通过消费者组读取并确认每个条目的流、[可靠列表](../redis/queues.md)，以及一个 Pub/Sub 频道。每种形式都分别在单机服务器、集群和 Sentinel 之后的主节点上运行。在集群上，Pub/Sub 一行使用分片形式，即 `SSUBSCRIBE` 和 `SPUBLISH`，集群上用的正是这种形式。

## 代码的开销 {/* #cost-of-the-code */}

第二张图统计一条消息在服务线程上的开销：稳态下 callgrind 测得的指令数和 DHAT 测得的内存分配次数。每个场景都是一个基于 [`RedisBroker`](api:ruststream_fred::RedisBroker) 的服务，连接测试环境的单机服务器。服务运行在单线程运行时上，`fred` 也在同一线程上驱动它的连接，因此计数包括运行时、本 crate，以及 `fred` 编写命令和解析应答的工作。服务器是另一个进程，不计入。`冷启动` 是连接连接池、创建组、打开订阅并取得第一次投递一次性付出的开销。

流场景用 `XREADGROUP` 读取消费者组，用 `XACK` 结算，用 `XADD` 应答。可靠列表用 `BLMOVE` 认领每个条目，用 `LREM` 结算它。Pub/Sub 场景读取一个频道或一个模式，不结算任何东西。`.pipeline()` 场景把一次读取的结算一起发送，它的处理器每次投递排入一条 `INCR`；`.pipeline().atomic()` 把每次投递的段包进 `MULTI` 和 `EXEC`，[窗口](../window.md)一页用图说明了这一点。

## 结果 {/* #results */}

<BenchmarkResults labels={labels} details="/architecture/benchmarks/methodology/" />

## 机器 {/* #the-machine */}

<BenchmarkEnvironment labels={machine} />

数字来自本版本的 `docs/benchmarks/results.json`，由基准测试命令写入，测量时使用的构建配置就是上面记录的那一个。[方法论](methodology.md)说明它们是怎样测得的，[数字](numbers.md)说明可以从中得出什么结论。

<MovedSections />
