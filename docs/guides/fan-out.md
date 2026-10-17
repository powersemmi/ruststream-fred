---
sidebar_position: 6
description: "Broadcast a message to every running service with Redis Pub/Sub: channels, patterns, slow subscribers, sharded channels and what is lost."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Fan-out with Pub/Sub

This guide broadcasts messages to every running instance of a service with Redis Pub/Sub.

The example is a storefront.
Every instance refreshes its price cache when a price changes, follows order events through a pattern, and announces shipped orders.

## Every subscriber receives the message

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/fan_out.rs#broadcast"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/fan_out.rs#broadcast"
```

</TabItem>
</Tabs>

[`RedisPubSub`](../components/pubsub/index.md) subscribes to one channel.
A message published to the channel reaches every subscription connected at that moment: `refresh_price` and `reindex_price` both receive it, in every running instance.
Nothing is shared out and nothing is acknowledged; the outcome a handler returns settles nothing.

Redis keeps no copy.
A message published while no subscription is connected is gone, and so is one published to an instance that is restarting.
A subscription opens only once Redis has routed its channel to it, so a message published after the service has started is not lost.

## A slow subscriber

Redis pushes each message as soon as it is published, whether the handler is ready or not.
The subscription holds what the handler has not taken yet, up to its [buffer](../components/pubsub/buffer.md): 32 messages unless `buffer` sets another size.
Once the buffer is full, each new message drops the oldest, and the subscription logs a warning with the channel and the number it lost.
`refresh_price` sets a buffer of a thousand messages, so a burst of price changes waits for it instead of being cut.
The buffer is reserved when the subscription opens, about 600 bytes per message whatever the payloads weigh.

## Patterns

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/fan_out.rs#pattern"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/fan_out.rs#pattern"
```

</TabItem>
</Tabs>

[`RedisPubSubPattern`](../components/pubsub/pattern.md) subscribes to a glob with `PSUBSCRIBE`, here every channel under `orders.events.`.
The handler reads the channel a message came on from the [Pub/Sub context](../components/context/pubsub-context.md) with `Ctx<keys::Channel>`.
`keys::FromPattern` tells whether the message matched through a pattern.

## Publishing to a channel

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/fan_out.rs#announce"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/fan_out.rs#announce"
```

</TabItem>
</Tabs>

`announce_shipment` reads shipments off a stream and replies with an `OrderEvent` on the channel `orders.events.shipped`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/fan_out.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/fan_out.rs#app"
```

</TabItem>
</Tabs>

The reply leaves through [`pubsub::Publish`](../components/publishing/pubsub-publish.md), which sends `PUBLISH`.
The broker's default publisher treats a name that no subscription of this service reads as a stream key and sends `XADD` to it.
The pattern subscription does not read `orders.events.shipped` by name, so the registration names the Pub/Sub policy.
The pattern subscription in the same service receives the announcement.

A pattern names no channel a copy could be published to.
So `track_order`'s registration names one with `out_retry(..).to("orders.events.retry")`, and the service does not start without it.
That channel matches the pattern, so a retried event comes back to the handler, in every running instance.

## Sharded channels on a cluster

On a [cluster](../components/broker/cluster.md), a classic `PUBLISH` goes to every node.
[`PubSubMode::Sharded`](../components/pubsub/pubsub-mode.md) uses `SSUBSCRIBE` and `SPUBLISH` instead: a channel lives on the node that owns its hash slot, so the traffic spreads across the cluster.
It needs Redis 7 or later and has no patterns.
The two modes do not reach each other, so the subscription and the publish policy name the same mode: `RedisPubSub::new(..).mode(PubSubMode::Sharded)` and `pubsub::Publish::new().mode(PubSubMode::Sharded)`.

## When the connection drops

The broker reconnects after a dropped connection, a restart of Redis or a failover, and every Pub/Sub subscription subscribes again.
The messages published while it was away are not delivered.
A sharded subscription is not restored after a resharding moves its slot to another node.

## When to use a stream instead

Pub/Sub suits what is useful only now: a cache to refresh, a live dashboard, a notice to every instance.
When each reader must receive every message, including the ones published while it was down, use a stream with one [consumer group](consumer-groups.md) per reader.
Each group reads every entry, and an entry stays in the stream until it is trimmed.

The tests in the example file publish a price change and a shipment and check that every subscription received what was published.
