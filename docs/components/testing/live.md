---
sidebar_position: 1
title: Live
description: "TestApp::start_live runs the same test body against a Redis server, for scripts, failover, authentication and TLS."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Live

[`TestApp::start_live`](api:ruststream::testing::TestApp::start_live) runs the same test body against a Redis server, for scripts, failover, authentication and TLS.

It connects the broker through its ordinary connect, so the client dials the server, logs in and runs the handshake.
The body is the one an in-process test runs; only the start call differs.
A publish, `settle` and `advance` wait on the real clock until every subscription has handled what was published to it.
The wait lasts ten seconds at most, and [`start_live_within`](api:ruststream::testing::TestApp::start_live_within) sets another bound.

The live mode is for what the in-process Redis leaves out: Lua scripts and functions, a [cluster](../broker/cluster.md) under resharding, a [sentinel](../broker/sentinel.md) failover, [authentication](../broker/authentication.md) and [TLS](../broker/tls.md).
A server keeps what earlier runs left in it, so a run reads keys no other test writes.

In the example, `live::url` returns the server's address from `REDIS_TEST_URL`, and the test returns early without one, so `cargo test` passes on a machine with no server.
A job that stood a server up sets `RUSTSTREAM_REQUIRE_LIVE=1`, and a missing address then fails the test instead of skipping it.
The repository's stand is `docker-compose.test.yml`, which `just brokers-up` starts.
`just test-brokers` starts it, runs every suite against it with `RUSTSTREAM_REQUIRE_LIVE=1`, and stops it.
Each topology has a variable of its own:

| Variable | Server | Stand address |
| --- | --- | --- |
| `REDIS_TEST_URL` | a standalone Redis 7 | `redis://127.0.0.1:6379` |
| `REDIS_84_TEST_URL` | a standalone Redis 8.4, for [`claiming`](../streams/claiming.md) | `redis://127.0.0.1:6386` |
| `REDIS_AUTH_TEST_URL` | a password-protected Redis 7 with an ACL user | `redis://127.0.0.1:6385` |
| `REDIS_CLUSTER_TEST_URL` | a seed node of a cluster | `127.0.0.1:7000` |
| `REDIS_SENTINEL_TEST_URL` | a sentinel watching the primary `mymaster` | `127.0.0.1:26379` |

## Example

The two tests share one body: one in process, one against the server at `REDIS_TEST_URL`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_testing_live.rs#start-live"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_testing_live.rs#start-live"
```

</TabItem>
</Tabs>
