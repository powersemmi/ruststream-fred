---
title: 消费者组
sidebar_position: 1
description: "消费者组用一个游标读取流，把已投递但无人确认的每个条目记在自己的待处理条目列表里。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "流上的一个消费者组：它的游标、它的待处理条目列表，以及在两者之间移动条目的命令",
  "text": "以文字阅读本图",
  "stream": "流 orders，在组 workers 眼中",
  "acked": "已确认",
  "pending": "待处理",
  "new": "新条目",
  "cursor": ["游标：", "last-delivered-id"],
  "list": ["待处理条目列表：", "3-0 给 c1，4-0 给 c2"],
  "reader": ["消费者 c1", "读取新条目"],
  "acker": ["消费者 c2", "确认条目"],
  "claimer": ["消费者 c3", "认领空闲条目"],
  "seek": "XGROUP SETID 把游标前移或后移：这就是 seek",
  "cursorAt": "指向",
  "read": "XREADGROUP",
  "ack": "XACK",
  "claim": "认领"
};

# 消费者组

消费者组用一个游标读取流，并把它投递过、但还没有人确认的每个条目记在自己的待处理条目列表里。

<Diagram name="pel" words={words} />

带 id `>` 的 `XREADGROUP` 返回组游标之后的条目，并移动游标。这个游标就是组的 `last-delivered-id`。每个条目只交给读取它的那个消费者。组把条目记入待处理条目列表（PEL），同时记下该消费者、投递时间和投递次数。`XACK` 把条目从列表中移除。条目仍留在流里：确认是组的记账，而不是删除。

订阅启动时，本 crate 用 `XGROUP CREATE` 和 `MKSTREAM` 创建组，因此尚不存在的流会先被创建为空流。已经存在的组保持原样。新组的游标从哪里开始，由 [`StreamStart`](api:ruststream_fred::stream::StreamStart) 决定。

消费者死掉后，它的条目留在列表里。条目空闲超过给定时间后，有两条命令可以把它交给另一个消费者。`XAUTOCLAIM` 扫描列表，找出这样的条目并转移它们。[`RedisStream::reclaim`](api:ruststream_fred::stream::RedisStream::reclaim) 按这种方式读取，并向 `XPENDING` 查询认领到的条目的投递次数。在 Redis 8.4 及更高版本上，`XREADGROUP` 接受 `CLAIM` 选项：一次调用先返回空闲条目，再返回新条目，每个条目都带有空闲时间和投递次数。[`RedisStream::claiming`](api:ruststream_fred::stream::RedisStream::claiming) 按这种方式读取，认领式订阅在较旧的服务器上拒绝启动。

`XGROUP SETID` 移动游标。本 crate 的 [`RedisGroupSeeker`](api:ruststream_fred::RedisGroupSeeker) 使用这条命令，因此一次 seek 重新定位的是整个组，而不是单个消费者。按旧游标选出的读取结果会被丢弃，下一次读取从新游标开始。

这些命令都是 Redis 自己的：[XREADGROUP](https://redis.io/docs/latest/commands/xreadgroup/)、[XACK](https://redis.io/docs/latest/commands/xack/)、[XAUTOCLAIM](https://redis.io/docs/latest/commands/xautoclaim/) 和 [XGROUP SETID](https://redis.io/docs/latest/commands/xgroup-setid/)。
