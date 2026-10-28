---
title: Design
sidebar_position: 2
description: "Where ruststream-fred sits: between the RustStream runtime and the fred client, a client of its own per subscription, one shared pool."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "The service, the RustStream runtime, this crate, the fred client and Redis, top to bottom",
  "text": "The diagram as text",
  "service": "Your service",
  "handlers": ["#[subscriber]", "handlers"],
  "app": ["RustStream::new(..)", ".with_broker(..)"],
  "core": "RustStream core",
  "runtime": "Runtime: dispatch, decode, settlement",
  "crate": "ruststream-fred",
  "subscribers": ["Subscribers", "of streams, lists", "and channels"],
  "publishers": ["Publishers", "of the same", "three forms"],
  "descriptors": ["RedisStream", "RedisList", "RedisPubSub"],
  "window": ["Pipeline window", "of .pipeline()"],
  "broker": ["RedisBroker", "topology, pool, credentials"],
  "client": "fred",
  "reader": ["A client of its own", "per subscription"],
  "pool": ["Pool", "4 connections"],
  "redis": "Redis or Valkey: standalone, cluster or sentinel",
  "mounted": "mounted",
  "polls": "polls",
  "publishes": "publishes"
};

# Design

Where `ruststream-fred` sits: between the RustStream runtime and the fred client, a client of its
own per subscription, one shared pool.

<Diagram name="design" words={words} />

The crate implements the core's broker traits over
[fred](https://docs.rs/fred/10.1.0/fred/). The runtime above it is the same for every broker.
Everything Redis-specific is in this crate: the descriptors of the three transports, what an
acknowledgement means on each, where a retry copy goes, and the pipeline window.

A descriptor resolves once, at startup, against the connected broker, and yields a subscriber.
The subscriber is a stream of deliveries that the runtime polls. The runtime decodes each
delivery, runs the handler and settles the delivery with its outcome. A publish policy pairs with
the connected broker into a publisher.

Each subscription reads on a client of its own, so a blocked read never holds up a publish.
Publishes, settlements and the commands a handler sends share the pool.
[Lifecycle](lifecycle.md) draws when each client opens and closes, and [Path](path.md) the way of
one entry.

The headers travel beside the payload in an envelope, a type the broker names and a descriptor or
a publish policy overrides. By default a stream entry carries the payload in a field named
`_payload` and each header in a field `h:<name>`, and a list entry and a channel message carry the
headers in a binary frame around the payload. A delivery finds its payload without reading the
headers and reads them when a handler first asks.
