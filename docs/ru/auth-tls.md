# Аутентификация и TLS {#authentication-and-tls}

URL отдельного сервера содержит учётные данные (`redis://user:pass@host`); в списке seed-узлов
кластера или sentinel места для них нет. `.credentials(user, pass)` задаёт их на любой топологии:

```rust
--8<-- "crates/ruststream-fred/examples/fred_auth.rs:credentials"
```

`.password(..)` задаёт `AUTH` только с паролем - старую форму `requirepass`, без ACL-пользователя:

```rust
--8<-- "crates/ruststream-fred/examples/fred_auth.rs:password"
```

Заданные так учётные данные перекрывают те, что стоят в URL отдельного сервера.

## TLS {#tls}

TLS дают три фичи, все выключенные по умолчанию: `tls-rustls` (rustls с aws-lc-rs),
`tls-rustls-ring` (rustls с ring) и `tls-native-tls`. С одной из них включённой `.tls(..)` принимает
`TlsConfig` или `TlsConnector` на любой топологии, а брокер на отдельном сервере может включить TLS
ещё и URL со схемой `rediss://` или `valkeys://`:

```rust
--8<-- "crates/ruststream-fred/examples/fred_tls.rs:tls"
```

## Ещё фичи аутентификации {#further-auth-features}

Ещё две фичи аутентификации, тоже выключенные по умолчанию:

- `sentinel-auth` добавляет `.sentinel_credentials(user, pass)` и `.sentinel_password(pass)` -
  учётные данные, которыми сервис аутентифицируется на самих sentinel, а не на узлах с данными.
- `credential-provider` добавляет `.credential_provider(provider)` - колбэк, который выдаёт и может
  менять имя пользователя и пароль на каждом `AUTH` или `HELLO` (аутентификация в духе IAM). Он имеет
  приоритет над статическими учётными данными.

Для настроек, до которых эти билдеры не дотягиваются (политика переподключения, тюнинг
производительности, свой TLS), соберите `Pool` из fred сами и оберните его в
`RedisBroker::from_pool`.
