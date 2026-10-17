---
title: AsyncAPI
description: "With asyncapi the generated document carries an x-ruststream-redis extension that says which structure carries a channel and how."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# AsyncAPI

With the `asyncapi` feature the generated document carries an `x-ruststream-redis` extension that says which structure carries a channel and how.

The core builds the AsyncAPI document from the app, before anything connects.
The specification has a `redis` binding, but its objects carry no fields, and a binding key is a closed list.
So this crate writes an extension instead, `x-ruststream-redis`, at the level a binding sits at.
The [server](./server.md) reports the host and port clients dial.
A [channel](./channels.md) a service reads reports the Redis structure behind it and how the subscription reads it.
A channel a service [publishes to](./operations.md) reports where the publish lands and how.

Everything comes from the service's own declarations: the broker, the descriptors, the policies and the destinations the mount sites resolved.
The Redis server's version stays out, because the client learns it only in the handshake.
The document is published and shared, so it carries no credential.
The binary `#[ruststream::app]` writes prints the document with `asyncapi gen`.
This crate's `asyncapi` feature turns on the core's `asyncapi` feature with it.
The core's feature alone produces a document without the extension.

## Example

The dependency line:

```toml
ruststream-fred = { version = "0.7", features = ["asyncapi"] }
```

The service registers its broker under the label `redis`, the key of its server in the document.
`cargo run --example redis_asyncapi --features asyncapi -- asyncapi gen` prints the document.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_asyncapi.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_asyncapi.rs#app"
```

</TabItem>
</Tabs>
