# 基准测试 { #benchmarks }

在 Redis 客户端和你的处理器之间，框架在每条消息上都要花时间：读取、解码、分发、确认。这一页说明
它花了多少，参照物是同样的活用 `fred` 手写一遍。

同一个进程把一个场景跑两遍：一遍是 RustStream 服务，一遍是客户端上的循环。其余一切都保持相同：
连接池和它的大小、消费者组和消费者名字、带着 `COUNT` 和 `BLOCK` 的读取命令、确认的位置、解码成
同一个类型、载荷字节、tokio 运行时和构建。这套流程属于框架本身，写在
[RustStream 基准测试页](https://powersemmi.github.io/ruststream/latest/zh/benchmarks/#methodology)
上；这一页公布它在这台机器上得出的结果。

一共测三个场景，对应这个 crate 提供的三种投递形态：逐条 ack 的 Redis Streams 消费者组、可靠模式
的列表工作队列，以及一个 Pub/Sub 频道。

## 数字 { #the-numbers }

十一组交错配对的中位数，括号里是观察到的离散范围。越大越好。

<div id="benchmark-results" data-benchmark-results="../../benchmarks/results.json" data-benchmark-labels='{"loading": "正在加载已公布的结果……", "scenario": "场景", "raw": "裸客户端", "framework": "RustStream", "overhead": "开销", "indistinguishable": "无法区分", "brokerBound": "受 Broker 限制", "unavailable": "已公布的结果加载失败。", "cpu": "CPU", "architecture": "架构", "cpu_frequency": "频率", "cores": "核心", "memory": "内存", "memory_speed": "内存速率", "os": "操作系统", "broker": "Broker", "rustc": "Rust", "profile": "构建配置", "features": "feature", "rustflags": "RUSTFLAGS", "versions": "版本", "measured": "测量日期"}'></div>

这张表每次打开页面时都从下面那份文档读取，所以它显示的是最近一次运行，别的都不是。

在 Redis 上确认一次投递要花掉一条自己的命令，流的条目是 `XACK`，列表的条目是 `LREM`；而对一台走
回环地址的服务器来说，这样一条命令要几十微秒，比一次投递在这个 crate 里花的时间高一个数量级。
消费者把它的测量窗口花在等待套接字上，所以这两行带着「受 Broker 限制」的标记：框架是在消费者
本来就要付的那段等待里做自己的活。对一个逐条确认的消费者来说，这是真实的结果，同时它也只是分发
开销的下界，而不是对它的测量。

Pub/Sub 不做任何确认，框架自身的活在这一行才有地方显现：那里的一次投递就是一次套接字读取、一次
解码和一次处理器调用，开销比一次流的投递低一个数量级。

同一次运行的机器可读形式在
[`benchmarks/results.json`](https://powersemmi.github.io/ruststream-fred/latest/benchmarks/results.json)，
框架的站点用它拼出跨 Broker 的汇总表。

## 机器 { #the-machine }

<div id="benchmark-machine"></div>

构建标志和数字一起公布，因为它们会改变这些数字。用 `-C target-cpu=native` 构建出的二进制给出的
结果，换一台机器就复现不了，所以这条 recipe 在构建前先把这个变量清空。

## 这些数字不代表什么 { #what-they-do-not-mean }

这里只有一个消费者、一个键、一个很小的消息体，以及一台跑在回环地址上的服务器。它测的是一次投递
在这个 crate 里的开销，不是 Redis 能扛多少。这里的一行也不能拿去和另一个 Broker 公布的一行比较：
不同的传输在每条消息上做的事并不一样。

一次运行的测量窗口从第一次投递开始，到最后一个处理器返回为止，两半都是这样。框架在处理器结束
之后才确认投递，而这个时刻处理器自己看不到，所以整场运行里的这一次确认，在两边都落在数字之外。

这次运行把服务器的 append-only 日志关掉了。要测的是一次投递的开销，不是服务器底下的磁盘，落在
一对里某一半上的 `fsync` 是一种两边都不属于的噪声。把这个日志留着开的服务要为它付出代价，而且
两边付得一样多。

Pub/Sub 的数字取自一个从不等待消费者的发布者。Redis Pub/Sub 会把消费者来不及取走的消息丢掉，而
不是排进队列，所以这一行报告的是一个饱和的消费者每秒处理掉多少次投递。

这些数字是一台机器在某一天的快照。它们按需重测，从不放进 CI：共享 runner 的噪声比这一页要讲的
差值还大。

## 自己跑一遍 { #running-it-yourself }

```bash
just bench
```

这条 recipe 从 `docker-compose.test.yml` 起停测试台，跑完所有场景，然后把测到的结果写回
`docs/benchmarks/results.json`。它要花十分钟左右，并且需要整台机器。消息条数不是固定的：一次试探
运行会把它定下来，使得每一次被测量的运行在所在机器上都不短于五秒。
