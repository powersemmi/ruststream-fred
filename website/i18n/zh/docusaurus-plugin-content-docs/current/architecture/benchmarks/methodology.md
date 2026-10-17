---
title: 方法论
description: "ruststream-fred 如何测量自己：三个循环、在真实 Redis 服务器上的运行、受 Broker 限制的判定计算，以及代码计数。"
---

# 方法论

`ruststream-fred` 如何测量自己：三个循环、在真实 Redis 服务器上的运行、受 Broker 限制的判定计算，以及代码计数。流程就是框架自己的流程（[RustStream 方法论](https://powersemmi.github.io/ruststream/latest/zh/architecture/benchmarks/methodology/)），本页说明它在 Redis 上是什么样子。

## 三个循环 {/* #the-three-loops */}

一个进程把每个场景跑三遍，作为三个循环，它们只有一处不同：承载消息的是什么。

- **裸客户端。** 一个 `fred` 上的循环：读取、解码、确认。
- **Broker crate。** 本 crate，上面什么也没有：Broker 建立连接，描述符打开一个订阅，一个循环从它的流中取出投递、解码并 ack。
- **RustStream。** 用户写的服务：一个 `#[subscriber]` 处理器及其外围的应用。

其余一切在三个循环中都相同：连接池及其大小、消费者组和消费者名、带 `COUNT` 和 `BLOCK` 的读取命令、ack 的位置、解码成同一个类型、载荷字节、带四个工作线程的 tokio 运行时，以及二进制程序。三个循环都由同一个流水线化的 `fred` 发布者提供消息，因此它们之间的差别都在消费这一侧。

消息体是 512 字节的 JSON。

## 运行 {/* #the-run */}

- **消息条数来自一次探测**，它测量裸循环的速率，因此在任何机器上，测量的运行都至少持续五秒。
- **在三个循环中，计时窗口都从第一次投递开始，到最后一次投递被取走时结束。**
- **各循环交替进行**：裸、crate、RustStream，共三轮。每个循环报告它最佳、中位和最差的一轮。

当裸客户端在运行中一直在等套接字时，该行标为 `受 Broker 限制`。一次探测在每种拓扑上为一条 `EXISTS` 及其应答计时，往返时间与机器信息一起公布，因此这个计算可以核对。结算一个流条目或列表条目要花一条自己的命令，即 `XACK` 或 `LREM`。在回环接口上，这次往返比一次投递在本 crate 内部花的时间更长。这些行带有该标记。Pub/Sub 不结算任何东西，套接字之上的工作在这一行才显现出来。

## 代码计数 {/* #the-code-count */}

[基准测试](/architecture/benchmarks/)一页开销图中的每个场景，都是用户写的服务，构建时不带 `testing` 特性。消息在测量区间打开之前由另一个线程追加，因此生产消息不计入。

指令数来自 callgrind，内存分配来自 DHAT。一个场景依次在一次投递、1000 次、2000 次上运行。后两次之差是一条消息的稳态开销，单次投递就是冷启动。

服务器是真实的，所以计数在各次运行之间会有少许变动。六次运行中，同一场景的指令总数彼此相差在 0.4% 以内，内存分配只相差几个块。每个内存分配下限取见到的最高计数，再加 0.1% 的余量。场景的内存分配超过它声明的下限时，`just bench-code` 失败；加上 `--baseline=main` 时，指令数多出百分之二以上也会失败。

## 自己运行 {/* #running-it-yourself */}

```bash
just bench
```

这条命令按 `docker-compose.test.yml` 启动测试环境，运行所有场景，停止测试环境，并重写 `docs/benchmarks/results.json`。它大约需要十分钟，并且需要独占机器。它会清空 `RUSTFLAGS`，因此二进制程序不会绑定到构建它的 CPU 上。构建配置与数字一起记录在这份文档中。

```bash
just bench-code
```

代码计数启动同一套测试环境，在 valgrind 下针对它的单机服务器统计各个场景，停止测试环境，并重写同一份文档中的 `code` 部分。它需要 valgrind，并自行安装基准测试运行器。在 `main` 上运行 `just bench-code --save-baseline=main` 记录基线，在分支上运行 `just bench-code --baseline=main` 与它对照。
