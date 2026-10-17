---
sidebar_position: 11
title: Errors
description: "RedisError is the one error type of the crate: connect, publish, subscribe, stream reads, shutdown, transactions and refused options."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Errors

[`RedisError`](api:ruststream_fred::RedisError) is the one error type of the crate: connect, publish, subscribe, stream reads, shutdown, transactions and refused options.

Each variant names the step that failed.
The `fred` error behind it is its `source`, so the client library's types stay out of the crate's API.

| Variant | When |
|---|---|
| `Connect` | the address could not be parsed, the server did not answer, or the login was refused |
| `Publish` | the server refused a write, such as a list push to a key that holds a stream (`WRONGTYPE`) |
| `Subscribe` | a consumer group could not be created, or a subscription could not open |
| `Stream` | a stream read or an acknowledgement failed |
| `ShutDown` | a publisher, a delivery or a transaction was used after the broker [shut down](./shutdown.md) |
| `InvalidOptions` | the settings cannot work together, such as a bare name with no [default group](./default-group.md) |
| `ServerTooOld` | a [`claiming`](../streams/claiming.md) subscription met a server older than Redis 8.4 |
| `TransactionBusy`, `NoTransaction` | a [transaction](../transactions/index.md) was begun twice on one publisher, or ended with none open |
| `Flush` | a [pipelined](../pipelining/index.md) window failed to send what it owed |

A service meets the error in two places.
A failed startup carries it as the source of the runtime's error, and the service exits before it reads anything.
A publish from a handler returns it inside the core's `PublishError`, and the handler decides how the delivery settles.
The enum is `#[non_exhaustive]`, so a `match` on it ends with a wildcard arm.

## Example

A ledger entry the server refuses is logged with its cause, and the order is settled as a retry.
The subscription claims the order back once it has been idle thirty seconds.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_broker_errors.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_broker_errors.rs#handler"
```

</TabItem>
</Tabs>
