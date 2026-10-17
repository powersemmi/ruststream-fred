---
sidebar_position: 11
description: "Run a Redis service in production: ACL users, TLS, rotating credentials, sentinel, pool size, reconnection after a dropped connection, and shutdown."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Running in production

A managed Redis asks a service for an ACL user and TLS, and production asks it to survive a restart of the server.
This guide configures the broker for both.

## ACL users and credentials from the environment

{/* single-form: the broker's configuration is the same code with and without the macros */}
```rust title="src/main.rs" reference="examples/guide/production.rs#broker"
```

The deployment provides the address and the credentials in environment variables, so no secret sits in the source.
[`credentials`](../components/broker/authentication.md) sets an ACL user and its password, and it works on every topology.
It is the only way to authenticate a cluster or a sentinel set, whose seed lists carry no password.
On a standalone server it wins over a user and a password written into the URL.
[`password`](api:ruststream_fred::RedisBroker::password) is the older `AUTH` with a password and no user, for a server configured with `requirepass` alone.

## The service

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/production.rs#service"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/production.rs#service"
```

</TabItem>
</Tabs>

The handlers do not know how the broker authenticates.
The tests start the same `service` with test credentials under `TestApp::start`; the in-process Redis checks no password, so the live tests log in to a server with an ACL user.

## TLS

```toml title="Cargo.toml"
[dependencies]
ruststream-fred = { version = "0.7", features = ["tls-rustls"] }
```

TLS is one of three features, mapped onto the TLS backends of `fred`: `tls-rustls` (rustls on `aws-lc-rs`), `tls-rustls-ring` (rustls on ring) and `tls-native-tls` (the platform's TLS library).
With one of them on, a standalone URL that starts with `rediss://` or `valkeys://` connects over TLS.
[`tls`](../components/broker/tls.md) takes a `TlsConnector` on any topology, which is how a cluster or a sentinel set gets TLS, and how a service sets its own trust roots or a client certificate.
`TlsConnector::default_rustls()` uses the system's trust roots.

## Rotating credentials

Managed Redis often issues short-lived passwords, such as an IAM token on ElastiCache.
The `credential-provider` feature adds [`credential_provider`](../components/broker/credential-provider.md): a callback that returns the user and the password for each `AUTH` or `HELLO`.
It can also log in again at an interval, before the token expires.
It wins over static credentials.

## Sentinel

{/* single-form: the broker's configuration is the same code with and without the macros */}
```rust title="src/main.rs" reference="examples/guide/production.rs#sentinel"
```

[`RedisBroker::sentinel`](../components/broker/sentinel.md) names the primary the sentinels monitor and the sentinels themselves.
The client asks the sentinels where the primary is, and asks again after a failover.
`credentials` authenticates to the data nodes.
Sentinels with a password of their own take [`sentinel_credentials`](../components/broker/sentinel-credentials.md), from the `sentinel-auth` feature.

## Connections and the pool size

The broker keeps a [pool](../components/broker/pool.md) of connections for publishes, acknowledgements and the commands handlers send.
Its size is 4 unless `pool(n)` sets another, and `broker` above sets 8.
Every stream and list subscription reads on a connection of its own, and every Pub/Sub subscription listens on one, so a blocking read never holds up a publish.
A service therefore opens the pool plus one connection per subscription; size the server's client limit for that.

Raise the pool size when publishes and acknowledgements queue behind each other under load.
A subscription mounted with [`.pipeline()`](pipelining.md) settles a whole fetched batch in one round trip, which takes load off the pool.

## Reconnection

Every connection the broker opens reconnects after the server drops it, as on a restart or a failover.
The first attempt follows after 100 ms, each next one waits twice as long, up to 10 s between attempts, with no limit on their number.
A command issued meanwhile waits for the connection.

What a subscription does once its connection is back:

- A stream subscription reads on.
  If the server lost the consumer group, as on a restart that kept no data, the subscription creates the group again and reads the stream from its start.
- A Pub/Sub subscription subscribes again.
  Messages published while it was disconnected are lost, as Pub/Sub never keeps them.
- A sharded Pub/Sub subscription on a cluster is not restored after a resharding moves its channel's slot to another node.
  The server ends that subscription without dropping the connection, so nothing reconnects; restart the service after a resharding.

For another reconnection policy, or any other setting the builders do not reach, build a `fred` `Pool` yourself and wrap it with [`from_pool`](../components/broker/from-pool.md).
The subscriptions' own connections then reconnect by that pool's policy.

## Shutdown

The service stops on `SIGINT` or `SIGTERM`.
The runtime stops reading, lets the handlers settle what they hold, and [shuts the broker down](../components/broker/shutdown.md): every blocked read ends at once and every connection closes.
An entry a stream handler did not get to acknowledge stays pending, and a [reclaim](../components/streams/reclaim.md) or [claiming](../components/streams/claiming.md) subscription picks it up after a restart.

## The generated document

The [AsyncAPI document](../components/asyncapi/server.md) names the host and port of the server, never the credentials.
A broker built with `from_pool` reports no host, because the pool hides it.
