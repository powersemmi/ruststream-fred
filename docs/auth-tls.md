# Authentication and TLS

The standalone URL carries credentials (`redis://user:pass@host`), but the bare `cluster` /
`sentinel` seed lists cannot. Builders set them on every topology, mapping onto fred's config:

```rust
--8<-- "crates/ruststream-fred/examples/fred_auth.rs:credentials"
```

For a password-only `AUTH` (the legacy `requirepass` form, no ACL user) use `.password(...)`:

```rust
--8<-- "crates/ruststream-fred/examples/fred_auth.rs:password"
```

Credentials set programmatically override any in a standalone URL.

## TLS

TLS lives behind additive, off-by-default features that map onto fred's TLS backends - `tls-rustls`
(rustls with aws-lc-rs), `tls-rustls-ring` (rustls with ring), and `tls-native-tls`. With one
enabled, pass a `TlsConfig` (or any `TlsConnector`) on any topology; a standalone broker can also
use a `rediss://` / `valkeys://` URL:

```rust
--8<-- "crates/ruststream-fred/examples/fred_tls.rs:tls"
```

## Further auth features

Two further auth features are off by default:

- `sentinel-auth` adds `.sentinel_credentials(user, pass)` / `.sentinel_password(pass)` for
  credentials that authenticate to the sentinels, distinct from the data-node credentials.
- `credential-provider` accepts `.credential_provider(provider)`, a callback that supplies and can
  rotate the username/password on each `AUTH` / `HELLO` (IAM-style auth); it takes precedence over
  static credentials.

For full control (custom reconnection, performance, or TLS policy beyond these builders), build a
fred `Pool` yourself and wrap it with `RedisBroker::from_pool`.
