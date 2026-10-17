---
sidebar_position: 9
title: credential_provider
description: "credential_provider supplies the user and password on each AUTH or HELLO, so a rotated password reaches a running service."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# credential_provider

[`credential_provider`](api:ruststream_fred::RedisBroker::credential_provider) supplies the user and password on each `AUTH` or `HELLO`, so a rotated password reaches a running service.

A provider implements [`CredentialProvider`](api:ruststream_fred::CredentialProvider), `fred`'s trait re-exported by the crate.
Its `fetch` returns the user and the password, and the client calls it before every login: at startup, on each new connection and after each reconnect.
Its `refresh_interval` makes the client log in again on a schedule, so a short-lived password is replaced before it expires.
The provider wins over [`credentials`](./authentication.md) set on the same broker.

The trait is asynchronous through `async-trait`, so the service depends on that crate too.
It comes with the `credential-provider` feature.
A [`TestApp`](../testing/index.md) test runs no handshake, so the provider is not called there.

## Example

The dependency lines:

```toml
ruststream-fred = { version = "0.7", features = ["credential-provider"] }
async-trait = "0.1"
```

The provider reads the password of the user `worker` from a secret file the platform rotates, and logs in again every ten minutes.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_broker_credential_provider.rs#provider"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_broker_credential_provider.rs#provider"
```

</TabItem>
</Tabs>
