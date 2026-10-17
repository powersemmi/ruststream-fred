---
slug: /
sidebar_position: 0
description: "Guides to building Redis and Valkey services with ruststream-fred, the Redis broker for RustStream."
---

# Guides

`ruststream-fred` is the Redis and Valkey broker for [RustStream](https://powersemmi.github.io/ruststream/latest/),
built on the [fred](https://docs.rs/fred) client. A service reads Redis Streams through consumer
groups, a list as a work queue, or Pub/Sub channels, on a standalone server, a cluster or a sentinel
set. The guides take a Redis service from installation to production.
