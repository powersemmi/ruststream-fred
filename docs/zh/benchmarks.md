# 基准测试 { #benchmarks }

在 Redis 客户端和消息类型之间，这层适配在每次投递上都要花时间：读取、解码、确认。这一页说明它
花了多少，参照物是同样的活用 `fred` 手写一遍。

同一个进程把一个场景跑三遍。**裸客户端**是在 `fred` 上手写的循环。**ruststream-fred** 是在这个
crate 上手写的同一个循环：Broker、订阅、它产出的投递流和 ack，没有服务、没有处理器、也没有分发。
**RustStream 服务**是用户写的那个服务，通过真正的运行时启动。

由此得到两个差值，它们回答的是不同的问题。适配层相对裸客户端，是这个 crate 自己的消费者比它所
包装的客户端多花多少，这个数字由本仓库负责。服务相对裸客户端，是用户从头到尾要付的。两者之间的
空隙，是运行时在这个 Broker 之上多花的；之所以按 Broker 分别公布，是因为每个适配层都很薄，如果
运行时占的份额在各个 Broker 之间仍有差别，那差别就住在两者相接的地方：流怎么产出、投递怎么到达、
背压怎么传回消费者。

其余一切都保持相同：连接池和它的大小、消费者组和消费者名字、带着 `COUNT` 和 `BLOCK` 的读取命令、
确认的位置、解码成同一个类型、载荷字节、tokio 运行时和构建。这套流程属于框架本身，写在
[方法论](https://powersemmi.github.io/ruststream/latest/zh/benchmarks/#methodology)一节；
这一页公布它在这台机器上得出的结果。

一共测三种消费者形态，对应这个 crate 提供的三种投递形态：逐条 ack 的 Redis Streams 消费者组、可靠模式
的列表工作队列，以及一个 Pub/Sub 频道。每一种都分别在这个 crate 能连接的三种服务器形态上测量：单机
服务器、集群，以及 Sentinel 之后的主节点。三种形态上的订阅完全相同，底下不同的只是客户端的路由，
所以这些行之间的差别说的正是路由。唯一一处有意的例外：集群上的 Pub/Sub 一行用的是分片形态，即
`SSUBSCRIBE` 和 `SPUBLISH`，这才是集群实际使用的形态，而经典的 `PUBLISH` 会广播到每一个节点。

## 数字 { #the-numbers }

三个交错轮次中的最佳值，括号里是中位的一轮。越大越好。小于多次运行之间离散范围的差值，
按「无法区分」公布，而不是给出百分比。

<div id="benchmark-results" data-benchmark-results="../../benchmarks/results.json" data-benchmark-labels='{"loading": "正在加载已公布的结果……", "scenario": "场景", "raw": "裸客户端", "adapter": "ruststream-fred", "framework": "RustStream 服务", "adapterOverhead": "适配层相对裸客户端", "overhead": "服务相对裸客户端", "indistinguishable": "无法区分", "brokerBound": "受 Broker 限制", "instructions": "每条消息的指令数", "allocations": "每条消息的内存分配次数", "cold": "冷启动（指令 / 分配）", "unavailable": "已公布的结果加载失败。", "cpu": "CPU", "architecture": "架构", "cpu_frequency": "频率", "cores": "核心", "memory": "内存", "memory_speed": "内存速率", "os": "操作系统", "broker": "Broker", "rustc": "Rust", "valgrind": "valgrind", "profile": "构建配置", "features": "feature", "rustflags": "RUSTFLAGS", "versions": "版本", "measured": "测量日期"}'></div>

这张表每次打开页面时都从下面那份文档读取，所以它显示的是最近一次运行，别的都不是。

在 Redis 上确认一次投递要花掉一条自己的命令，流的条目是 `XACK`，列表的条目是 `LREM`；而对一台走
回环地址的服务器来说，这样一条命令要几十微秒，比一次投递在这个 crate 里花的时间高一个数量级。
消费者把它的测量窗口花在等待套接字上，所以这两行带着「受 Broker 限制」的标记：套接字之上的
一切，都是在消费者本来就要付的那段等待里做自己的活。对一个逐条确认的消费者来说，这是真实的
结果，同时它也只是这些开销的下界，而不是对它们的测量。

Pub/Sub 不做任何确认，套接字之上的活在这一行才有地方显现：那里的一次投递就是一次套接字读取、
一次拆信封和一次解码，开销比一次流的投递低一个数量级。

同一次运行的机器可读形式在
[`benchmarks/results.json`](https://powersemmi.github.io/ruststream-fred/latest/benchmarks/results.json)，
框架的站点用它拼出跨 Broker 的汇总表。

## crate 自身的代码 { #the-crates-own-code }

<div id="benchmark-code"></div>

第二张表是这个 crate 自身在每条消息上的开销，是数出来的，不是计时得来的：指令数由 callgrind 统计，
内存分配次数由 DHAT 统计。每个场景都是用户会写的那种服务，建立在 `RedisBroker` 上，对着测试台的
单机服务器启动，所以其中的每一条命令都是真实服务会发出的：`XREADGROUP` 读取 `RedisStream`
消费者组，`XACK` 确认投递，`XADD` 经由 `RedisPublish` 发出回复。

服务跑在单线程的 tokio 运行时上，`fred` 也在同一个线程上驱动它的连接。这个线程上的一切都计算在内：
框架、这个 crate，以及 `fred` 编写命令和解析回复的工作。服务器是另一个进程，不在数字里；内核处理
套接字调用的那一部分也不在。消息在被测的消费阶段开始之前，由另一个线程追加到流里，所以生产消息
也不计算在内。

指令数和分配次数都是稳态下每条消息的值：1000 次投递的运行和 2000 次投递的运行之间的斜率。最后一列
是连接连接池、创建消费者组、打开订阅并处理第一次投递一次性付出的开销。这些数字是绝对值，框架自身的
开销也算在内；框架单独的开销由核心库在它的
[基准测试页面](https://powersemmi.github.io/ruststream/latest/zh/benchmarks/)上公布。

服务连着真实的服务器，所以计数在两次运行之间会有少许浮动：六次运行里，同一场景的指令总数彼此相差
不超过 0.4%，分配次数在约 59000 次里相差不超过 8 次。因此每个场景的下限取见到的最大值，再加 0.1% 的
余量。`just bench-code` 在分配次数超过场景声明的下限时失败，加上 `--baseline=main` 时，
指令数多出百分之二以上也算失败；改变开销的合并请求要附上自己的数字。

## 机器 { #the-machine }

<div id="benchmark-machine"></div>

构建标志和数字一起公布，因为它们会改变这些数字。用 `-C target-cpu=native` 构建出的二进制给出的
结果，换一台机器就复现不了，所以这条 recipe 在构建前先把这个变量清空。

## 这些数字不代表什么 { #what-they-do-not-mean }

这里只有一个消费者、一个键、一个很小的消息体，以及一台跑在回环地址上的服务器。它测的是一次投递
在这个 crate 里、以及在它之上的运行时里的开销，不是 Redis 能扛多少。这里的一行也不能拿去和另一个 Broker 公布的一行比较：
不同的传输在每条消息上做的事并不一样。

一次运行的测量窗口从第一次投递开始，到取到最后一次投递为止，三个循环都是这样，所以整场运行里
的那一次确认，在哪一边都落在数字之外。

发布这一侧不在这些行里。三个循环都由同一个流水线化的 `fred` 发布者喂数据，好让它们之间的差别
都留在消费这一侧；这个 crate 自己的发布者花多少，是另一项测量。

测试台上的服务器跑在宿主机网络里，并且不做持久化：客户端和服务器之间没有端口代理，没有
append-only 日志，也没有快照。要测的是一次投递的开销，不是服务器前面的网桥，也不是它底下的磁盘。
把持久化留着开的服务要为它付出代价，而且两边付得一样多。

Pub/Sub 的数字取自一个从不等待消费者的发布者。Redis Pub/Sub 会把消费者来不及取走的消息丢掉，而
不是排进队列，所以这一行报告的是一个饱和的消费者每秒处理掉多少次投递。

这些数字是一台机器在某一天的快照。
它们由人手工重测，测量期间机器只跑这一项：这一页要讲的差值，比与其他任务共用的机器上的噪声还小。

## 自己跑一遍 { #running-it-yourself }

```bash
just bench
```

这条 recipe 从 `docker-compose.test.yml` 起停测试台，跑完所有场景，然后把测到的结果写回
`docs/benchmarks/results.json`。它要花十分钟左右，并且需要整台机器。消息条数不是固定的：一次试探
运行会把它定下来，使得每一次被测量的运行在所在机器上都不短于五秒。

```bash
just bench-code
```

这条 recipe 起停同一套测试台，在 valgrind 下对着它的单机服务器统计代码表，并重写同一份文档里的
`code` 部分。它需要 valgrind 和基准测试运行器：`cargo install --locked gungraun-runner --version =0.19.4`。
