---
sidebar_position: 6
title: Authentication
description: "credentials sets an ACL user and its password on every topology; password is the legacy AUTH with a password and no user."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Authentication

[`credentials`](api:ruststream_fred::RedisBroker::credentials) sets an ACL user and its password on every topology; [`password`](api:ruststream_fred::RedisBroker::password) is the legacy `AUTH` with a password and no user.

The client logs in on every connection it opens, the subscriptions' own included, and again after each reconnect.
`credentials` names an ACL user, which Redis 6 and later and Valkey check against the user's rules.
`password` logs in as the default user, the form a server configured with `requirepass` expects.
A standalone URL may carry the pair itself (`redis://worker:pass@host:6379`), and a pair set with `credentials` wins over it.
A [cluster](./cluster.md) seed list and a [sentinel](./sentinel.md) list carry no password, so `credentials` is how those topologies log in.

A login the server refuses fails startup.
The [generated document](../asyncapi/server.md) never carries the password.
A password that changes while the service runs comes from a [`credential_provider`](./credential-provider.md).

## Example

The service logs in as the ACL user `worker`, with a password from the environment.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_broker_auth.rs#broker"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_broker_auth.rs#broker"
```

</TabItem>
</Tabs>
