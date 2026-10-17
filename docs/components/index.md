---
sidebar_position: 0
description: "The parts of ruststream-fred a Redis service uses, one page per component."
---

# Components

The parts of `ruststream-fred` a Redis service uses, one page per component: the broker
([`RedisBroker`](api:ruststream_fred::RedisBroker)), its subscription descriptors for streams, lists
and Pub/Sub, its publish policies, the pipeline window and the in-process Redis a
[`TestApp`](api:ruststream::testing::TestApp) test runs on.
