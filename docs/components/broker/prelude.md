---
sidebar_position: 12
title: Prelude
description: "Each form has a prelude with its policy under the name Publish; ruststream_fred::prelude serves a file that mixes forms."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Prelude

Each form has a prelude with its policy under the name `Publish`; [`ruststream_fred::prelude`](api:ruststream_fred::prelude) serves a file that mixes forms.

A service on one form globs that form's prelude: [`stream::prelude`](api:ruststream_fred::stream::prelude), [`list::prelude`](api:ruststream_fred::list::prelude) or [`pubsub::prelude`](api:ruststream_fred::pubsub::prelude).
Each carries the core's own prelude, so `#[subscriber]`, `RustStream`, `HandlerOutcome` and `Out` come with it.
It adds [`RedisBroker`](./index.md), the form's descriptors and their options, the form's context types, and the capabilities the form offers.
The form's policy comes under the name every broker crate uses, `Publish`, so a mount site reads the same on every broker.
The stream prelude also brings `TransactionalPublish`, the seek types and the `keys` module of [context](../context/index.md) keys.

`ruststream_fred::prelude` is for a file that mixes forms.
It holds the same items for all three forms, with the policies under their prefixed names, `RedisPublish`, `RedisListPublish` and `RedisPubSubPublish`.
It also brings the `stream`, `list` and `pubsub` modules, so a mixed file writes `stream::Publish` beside `pubsub::Publish`.
It has no bare `Publish`, because one name would stand for three types.

A handler body names only the capability it needs, such as `Out<impl Publisher>`, so moving a handler between forms changes the descriptor and not the handler.
The one exception is a body that sets a per-message setting: it names [`RedisPublishOptions`](api:ruststream_fred::RedisPublishOptions) in its bound, from this crate's prelude.

## Example

A stream subscription replies onto a Pub/Sub channel, and a channel subscription hears the reply.
The one glob brings both descriptors and the `pubsub` module the reply policy comes from.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_broker_prelude.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_broker_prelude.rs#app"
```

</TabItem>
</Tabs>
