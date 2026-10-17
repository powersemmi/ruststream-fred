---
title: 队列
sidebar_position: 2
description: "可靠列表在处理器运行之前把每个条目移到处理中列表，处理器结算时再把它从那里移除。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "可靠列表：认领把条目移到处理中列表，结果在那里结算它，恢复集合把无主的条目送回队列",
  "text": "以文字阅读本图",
  "producer": "生产者",
  "queue": "jobs",
  "processing": "jobs.processing",
  "recovery": ["恢复集合", "按认领时间排序"],
  "handler": "处理器",
  "ack": ["ack：", "LREM"],
  "retry": ["retry：LPUSH", "推回，再 LREM"],
  "drop": ["drop：", "LREM"],
  "orphan": "每次读取先把空闲超过 min_idle 的认领送回队列",
  "push": "LPUSH",
  "claim": "BLMOVE",
  "delivered": "投递",
  "tracked": "ZADD",
  "sweep": "送回无主条目"
};

# 队列

可靠列表在处理器运行之前把每个条目移到处理中列表，处理器结算时再把它从那里移除。

<Diagram name="queue" words={words} />

列表是一个队列：`LPUSH` 从一端加入，弹出从另一端取，最早的条目最先离开。弹出的条目就从 Redis 中消失了。简单的 [`RedisList`](api:ruststream_fred::list::RedisList) 用 `BRPOP` 弹出，因此处理器失败或进程死掉时，条目就丢失了：投递是至多一次。

[可靠](api:ruststream_fred::list::RedisList::reliable)列表用认领代替弹出。`BLMOVE` 用一条命令把条目从队列移到处理中列表，处理器运行期间条目一直留在那里。处理中列表默认是 `<key>.processing`，订阅也可以指定别的名字。结果在处理中列表上结算条目：`LREM` 移除它，重试则先把它推回队列。投递是至少一次。Redis 在 [LMOVE](https://redis.io/docs/latest/commands/lmove/) 下描述了这一模式。

列表不记录谁在何时取走了条目，因此消费者在认领之后死掉，条目就留在处理中列表上。[`recovery_zset`](api:ruststream_fred::list::RedisList::recovery_zset) 保存这份记录。每次认领都加入一个有序集合，以认领时间为分值。每次读取先清扫早于 `min_idle` 的认领，用 `LREM` 和 `LPUSH` 把它们的条目移回队列，每次读取最多 128 个。

在集群上，两个列表必须散列到同一个槽，因为 `BLMOVE` 要访问两个键。两个键落在不同槽上的订阅拒绝启动。像 `{jobs}` 这样的哈希标签把两个键放到同一个槽上，默认的处理中键也随之落在这个槽。
