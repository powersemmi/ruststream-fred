---
sidebar_position: 11
description: "在生产环境运行 Redis 服务：ACL 用户、TLS、轮换凭据、Sentinel、连接池大小、断线后的重连，以及关闭。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 在生产环境运行

托管的 Redis 要求服务使用 ACL 用户和 TLS，生产环境则要求服务挺过服务器的重启。
本指南为这两点配置 Broker。

## ACL 用户与来自环境变量的凭据 {/* #acl-users-and-credentials-from-the-environment */}

{/* single-form: the broker's configuration is the same code with and without the macros */}
```rust title="src/main.rs" reference="examples/guide/production.rs#broker"
```

部署环境通过环境变量提供地址和凭据，所以源码中没有任何密钥。
[`credentials`](../components/broker/authentication.md) 设置一个 ACL 用户及其密码，它适用于每种拓扑。
集群和 Sentinel 组的种子列表不带密码，所以这是为它们认证的唯一方式。
在单机服务器上，它优先于写在 URL 中的用户和密码。
[`password`](api:ruststream_fred::RedisBroker::password) 是较旧的 `AUTH` 方式，只有密码没有用户，用于只配置了 `requirepass` 的服务器。

## 服务 {/* #the-service */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/production.rs#service"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/production.rs#service"
```

</TabItem>
</Tabs>

处理器不知道 Broker 如何认证。
测试用 `TestApp::start` 以测试凭据启动同一个 `service`；进程内 Redis 不检查密码，所以 live 测试用一个 ACL 用户登录服务器。

## TLS {/* #tls */}

```toml title="Cargo.toml"
[dependencies]
ruststream-fred = { version = "0.7", features = ["tls-rustls"] }
```

TLS 由三个 feature 之一提供，它们对应 `fred` 的 TLS 后端：`tls-rustls`（基于 `aws-lc-rs` 的 rustls）、`tls-rustls-ring`（基于 ring 的 rustls）和 `tls-native-tls`（平台的 TLS 库）。
启用其中一个之后，以 `rediss://` 或 `valkeys://` 开头的单机 URL 会通过 TLS 连接。
[`tls`](../components/broker/tls.md) 在任何拓扑上接收一个 `TlsConnector`：集群或 Sentinel 组用它获得 TLS，服务也用它设置自己的信任根或客户端证书。
`TlsConnector::default_rustls()` 使用系统的信任根。

## 轮换凭据 {/* #rotating-credentials */}

托管的 Redis 常常签发短期密码，例如 ElastiCache 上的 IAM 令牌。
`credential-provider` feature 加入 [`credential_provider`](../components/broker/credential-provider.md)：一个回调，为每次 `AUTH` 或 `HELLO` 返回用户和密码。
它还可以按一定间隔、在令牌过期之前重新登录。
它优先于静态凭据。

## Sentinel {/* #sentinel */}

{/* single-form: the broker's configuration is the same code with and without the macros */}
```rust title="src/main.rs" reference="examples/guide/production.rs#sentinel"
```

[`RedisBroker::sentinel`](../components/broker/sentinel.md) 指定 Sentinel 监控的主节点，以及这些 Sentinel 节点本身。
客户端向 Sentinel 询问主节点在哪里，故障转移之后会再问一次。
`credentials` 用于向数据节点认证。
有自己密码的 Sentinel 节点使用 [`sentinel_credentials`](../components/broker/sentinel-credentials.md)，它来自 `sentinel-auth` feature。

## 连接与连接池大小 {/* #connections-and-the-pool-size */}

Broker 维护一个[连接池](../components/broker/pool.md)，用于发布、确认和处理器发送的命令。
除非 `pool(n)` 设置了别的大小，否则池的大小是 4，上面的 `broker` 设为 8。
每个流订阅和列表订阅都在自己的连接上读取，每个 Pub/Sub 订阅都在自己的连接上监听，所以阻塞读取从不耽误发布。
因此，服务打开的连接数是连接池加上每个订阅一条；据此设置服务器的客户端数上限。

当负载下发布和确认互相排队时，就调大连接池。
用 [`.pipeline()`](pipelining.md) 挂载的订阅在一次往返中结算整批拉取的条目，从而减轻连接池的负担。

## 重新连接 {/* #reconnection */}

服务器断开连接后，例如重启或故障转移时，Broker 打开的每条连接都会重新连接。
第一次尝试在 100 毫秒之后，之后每次等待的时间加倍，两次尝试之间最多 10 秒，尝试次数不限。
期间发出的命令会等待连接恢复。

连接恢复之后，订阅会这样做：

- 流订阅继续读取。
  如果服务器丢失了消费者组，例如重启时没有保留数据，订阅会重新创建组，并从流的开头读取。
- Pub/Sub 订阅重新订阅。
  断开期间发布的消息会丢失，因为 Pub/Sub 从不保存消息。
- 重新分片把集群上分片 Pub/Sub 订阅的频道槽移到另一个节点之后，这个订阅不会恢复。
  服务器结束该订阅却不断开连接，所以不会触发任何重连；重新分片之后要重启服务。

如果需要别的重连策略，或者构建器无法设置的其他任何设置，就自己构建一个 `fred` 的 `Pool`，再用 [`from_pool`](../components/broker/from-pool.md) 包装它。
这样，订阅自己的连接也按该池的策略重连。

## 关闭 {/* #shutdown */}

服务在收到 `SIGINT` 或 `SIGTERM` 时停止。
运行时停止读取，让处理器结算手头的投递，然后[关闭 Broker](../components/broker/shutdown.md)：每个阻塞的读取立即结束，每条连接都被关闭。
流处理器还没来得及确认的条目保持待处理，重启之后由[回收](../components/streams/reclaim.md)订阅或[认领](../components/streams/claiming.md)订阅接手。

## 生成的文档 {/* #the-generated-document */}

[AsyncAPI 文档](../components/asyncapi/server.md)写明服务器的主机和端口，从不包含凭据。
用 `from_pool` 构建的 Broker 不报告主机，因为连接池把它隐藏了。
