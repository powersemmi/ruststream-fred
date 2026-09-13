# 认证与 TLS { #authentication-and-tls }

单机的 URL 里可以写凭据（`redis://user:pass@host`）；集群或 sentinel 的种子地址列表则没有地方放它们。
`.credentials(user, pass)` 在每种拓扑上设定凭据：

```rust
--8<-- "crates/ruststream-fred/examples/fred_auth.rs:credentials"
```

`.password(..)` 设定只有密码的 `AUTH`，也就是没有 ACL 用户的老式 `requirepass` 形式：

```rust
--8<-- "crates/ruststream-fred/examples/fred_auth.rs:password"
```

这样设定的凭据覆盖单机 URL 里的那一份。

## TLS { #tls }

三个默认关闭的 feature 提供 TLS：`tls-rustls`（rustls 配 aws-lc-rs）、`tls-rustls-ring`（rustls 配
ring）和 `tls-native-tls`。打开其中之一后，`.tls(..)` 在任何拓扑上都接受 `TlsConfig` 或
`TlsConnector`，而单机的 Broker 还可以用 `rediss://` 或 `valkeys://` URL 打开 TLS：

```rust
--8<-- "crates/ruststream-fred/examples/fred_tls.rs:tls"
```

## 其余的认证 feature { #further-auth-features }

另外两个认证 feature，同样默认关闭：

- `sentinel-auth` 增加 `.sentinel_credentials(user, pass)` 和 `.sentinel_password(pass)`，这是向
  sentinel 而不是向数据节点认证所用的凭据。
- `credential-provider` 增加 `.credential_provider(provider)`，这是一个回调，在每次 `AUTH` 或
  `HELLO` 时给出用户名和密码，并且可以轮换它们（IAM 风格的认证）。它的优先级高于静态凭据。

这些构建器够不到的设置（重连策略、性能调优、你自己的一套 TLS），自己构建一个 fred 的 `Pool`，再用
`RedisBroker::from_pool` 把它包起来。
