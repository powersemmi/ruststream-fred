---
sidebar_position: 7
title: TLS
description: "tls takes a TlsConnector or TlsConfig on any topology; a rediss or valkeys URL switches TLS on for a standalone server."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# TLS

[`tls`](api:ruststream_fred::RedisBroker::tls) takes a [`TlsConnector`](api:ruststream_fred::TlsConnector) or a [`TlsConfig`](api:ruststream_fred::TlsConfig) on any topology; a `rediss://` or `valkeys://` URL switches TLS on for a standalone server.

TLS comes with one of three features, all off by default.
`tls-rustls` builds on rustls with the `aws-lc-rs` crypto provider, `tls-rustls-ring` on rustls with `ring`, and `tls-native-tls` on the platform's own TLS library.
The features add to each other rather than exclude each other.
The crate re-exports `TlsConnector` and `TlsConfig` from `fred`, so a service names them without a dependency on `fred`.

`TlsConnector::default_rustls()` trusts the system's root certificates and presents no client certificate.
It reads the certificates from the system and can fail, so the connector is built in `main` before the service starts.
A connector built from a rustls `ClientConfig` carries a private certificate authority or a client certificate.
Every connection of the broker uses it, the subscriptions' own included.

A [`TestApp`](../testing/index.md) test runs no handshake, so it proves the service starts and works with the connector in place.

## Example

The dependency line:

```toml
ruststream-fred = { version = "0.7", features = ["tls-rustls"] }
```

The service reaches a cluster over TLS and logs in as the ACL user `worker`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_broker_tls.rs#broker"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_broker_tls.rs#broker"
```

</TabItem>
</Tabs>
