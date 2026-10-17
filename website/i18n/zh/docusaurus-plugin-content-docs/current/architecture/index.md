---
sidebar_position: 0
description: "Redis 如何投递消息，ruststream-fred 在 Redis 与 RustStream 运行时之间处于什么位置，以及这样做的开销。"
---

# 架构

本节用图说明 Redis 如何通过流、列表和 Pub/Sub 投递消息，`ruststream-fred` 在 Redis 与 RustStream 运行时之间处于什么位置，以及这给服务带来的开销。
