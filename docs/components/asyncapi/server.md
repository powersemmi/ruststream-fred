---
sidebar_position: 1
title: Server
description: "The server reports the host and port a client dials, with the scheme, credentials, database and query stripped."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Server

The server reports the host and port a client dials, with the scheme, credentials, database and query stripped.

A broker registered with `with_broker_labeled` is a server of the document, under its label.
Its `protocol` is `redis`.
Its `host` is the `host:port` of a [standalone](../broker/index.md) URL, or the first seed of a [cluster](../broker/cluster.md) or a [sentinel](../broker/sentinel.md) list.
A `redis://` or `rediss://` scheme, a `user:password@`, a `/2` database and a `?` query stay out of it.
An address without a port reports the topology's default, `6379`, or `26379` for a sentinel.

A broker built with [`from_pool`](../broker/from-pool.md) reports no host at all.
Its address sits inside the pool's own config, and an empty host would read as an address to dial.
A password set with [`credentials`](../broker/authentication.md) never reaches the document either.

## Example

The broker's URL carries a password and a database.
Its server in the document is `redis.internal:6379` with the protocol `redis`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_asyncapi.rs#server"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_asyncapi.rs#server"
```

</TabItem>
</Tabs>
