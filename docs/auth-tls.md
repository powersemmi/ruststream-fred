# Authentication and TLS

A standalone URL carries credentials (`redis://user:pass@host`); a cluster or sentinel seed list has
nowhere to put them. `.credentials(user, pass)` sets them on every topology:

```rust
--8<-- "crates/ruststream-fred/examples/fred_auth.rs:credentials"
```

`.password(..)` sets a password-only `AUTH`, the legacy `requirepass` form with no ACL user:

```rust
--8<-- "crates/ruststream-fred/examples/fred_auth.rs:password"
```

Credentials set this way override the ones in a standalone URL.

## TLS

Three off-by-default features carry TLS: `tls-rustls` (rustls with aws-lc-rs), `tls-rustls-ring`
(rustls with ring), and `tls-native-tls`. With one of them on, `.tls(..)` takes a `TlsConfig` or a
`TlsConnector` on any topology, and a standalone broker can also switch TLS on with a `rediss://`
or `valkeys://` URL:

```rust
--8<-- "crates/ruststream-fred/examples/fred_tls.rs:tls"
```

## Further auth features

Two more auth features, also off by default:

- `sentinel-auth` adds `.sentinel_credentials(user, pass)` and `.sentinel_password(pass)`, the
  credentials that authenticate to the sentinels rather than to the data nodes.
- `credential-provider` adds `.credential_provider(provider)`, a callback that supplies and can
  rotate the username and password on each `AUTH` or `HELLO` (IAM-style auth). It takes precedence
  over static credentials.

For settings these builders do not reach (a reconnection policy, performance tuning, a TLS setup of
your own), build a fred `Pool` yourself and wrap it with `RedisBroker::from_pool`.
