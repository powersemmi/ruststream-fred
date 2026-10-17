---
title: Lifecycle
sidebar_position: 4
description: "RedisBroker records configuration, connect opens the pool, each subscription opens a client of its own, and shutdown unblocks and closes them."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "The broker's three states as types, and what each transition does on Redis",
  "text": "The diagram as text",
  "states": "Broker",
  "redis": "On Redis",
  "unconnected": ["RedisBroker::", "standalone(url)"],
  "connected": ["ConnectedRedisBroker"],
  "closed": ["ClosedRedisBroker", "connections_closed"],
  "config": ["Configuration only,", "no I/O"],
  "pool": ["A pool of 4 connections:", "publishes, settles"],
  "subscriptions": ["Each subscription:", "its group created,", "a client of its own"],
  "unblock": ["Subscription dropped:", "CLIENT UNBLOCK, QUIT"],
  "quit": ["Waits for every read", "client, QUITs the pool"],
  "afterShutdown": ["A publisher or delivery that outlives the connection", "answers RedisError::ShutDown at once"],
  "connect": "connect(self)",
  "shutdown": "shutdown(self)"
};

# Lifecycle

`RedisBroker` records configuration, connect opens the pool, each subscription opens a client of
its own, and shutdown unblocks and closes them.

<Diagram name="lifecycle" words={words} />

[`RedisBroker::standalone`](api:ruststream_fred::RedisBroker::standalone) records the URL and
does no I/O, as do `cluster` and `sentinel`, so the service's builder stays synchronous. The
runtime connects the broker when the app starts. Connect turns the topology, the credentials and
the TLS settings into a `fred` configuration, and opens a pool of
[`pool`](api:ruststream_fred::RedisBroker::pool) connections, four by default. An unreachable
server fails the start. A broker built with
[`from_pool`](api:ruststream_fred::RedisBroker::from_pool) adopts the pool it was given and
dials it here, unless the service connected it already.

Publishes, settlements and the commands a handler sends go out on the pool. Each subscription
opens a client of its own when it subscribes. A stream or list subscription reads on it, because a
blocking read (`XREADGROUP` with `BLOCK`, `BLMOVE`, `BRPOP`) holds its connection until it returns,
and a publish queued behind it would wait out the whole block interval. A Pub/Sub subscription
receives its messages on it. A stream subscription creates its consumer group before it returns.

When a subscription is dropped, its client sends `CLIENT UNBLOCK` on `fred`'s side connection,
which ends a read still blocked on the server, and then `QUIT`. A blocked pop left running would
take the next entry for a subscription nobody reads.

[Shutdown](../components/broker/shutdown.md) marks the connection closed, waits until every read
client has closed, and sends `QUIT` to the pool.
[`ClosedRedisBroker`](api:ruststream_fred::ClosedRedisBroker) reports how many pool connections
it closed. A publisher, a seeker or a delivery that outlives the connection checks the
mark before it sends, and answers
[`RedisError::ShutDown`](api:ruststream_fred::RedisError::ShutDown) at once.
