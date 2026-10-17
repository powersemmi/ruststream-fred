---
slug: /
sidebar_position: 0
description: "使用 RustStream 的 Redis Broker ruststream-fred 构建 Redis 和 Valkey 服务的指南。"
---

# 指南

`ruststream-fred` 是 [RustStream](https://powersemmi.github.io/ruststream/latest/zh/) 的 Redis 和 Valkey Broker，
基于 [fred](https://docs.rs/fred) 客户端构建。服务可以通过消费者组读取 Redis 流，把列表当作工作队列，或者订阅 Pub/Sub 频道；服务器可以是单机、集群或 Sentinel 管理的主从组。这些指南带你把一个 Redis 服务从安装一路推进到生产环境。
