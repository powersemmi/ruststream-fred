---
sidebar_position: 8
title: sentinel_credentials
description: "sentinel_credentials and sentinel_password log in to the sentinels with a pair of their own, apart from the data nodes."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# sentinel_credentials

[`sentinel_credentials`](api:ruststream_fred::RedisBroker::sentinel_credentials) and [`sentinel_password`](api:ruststream_fred::RedisBroker::sentinel_password) log in to the sentinels with a pair of their own, apart from the data nodes.

A Sentinel deployment may protect the sentinels and the primary separately.
The client asks the sentinels where the primary is, logged in with `sentinel_credentials`, and then logs in to the primary with [`credentials`](./authentication.md).
`sentinel_password` is the form with a password and no user.
Both come with the `sentinel-auth` feature and apply to the [sentinel](./sentinel.md) topology only.

A [`TestApp`](../testing/index.md) test runs no handshake, so it proves the service starts and works with both pairs set.

## Example

The dependency line:

```toml
ruststream-fred = { version = "0.7", features = ["sentinel-auth"] }
```

The primary checks the user `worker`, and the sentinels the user `monitor`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_broker_sentinel_auth.rs#broker"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_broker_sentinel_auth.rs#broker"
```

</TabItem>
</Tabs>
