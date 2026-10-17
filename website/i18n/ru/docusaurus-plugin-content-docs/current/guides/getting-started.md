---
sidebar_position: 1
description: "Установка ruststream-fred, локальный Redis, первый подписчик на потоке Redis и его тест без сервера."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Начало работы

Это руководство устанавливает `ruststream-fred`, запускает локальный Redis и первый сервис на нём.

## Установка

```toml title="Cargo.toml"
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json", "logging"] }
ruststream-fred = "0.7"
serde = { version = "1", features = ["derive"] }
tracing = "0.1"

[dev-dependencies]
ruststream-fred = { version = "0.7", features = ["testing"] }
tokio = { version = "1", features = ["macros", "rt"] }
```

`ruststream` - это сам фреймворк. Фича `macros` даёт атрибуты, `json` - кодек по умолчанию, а `logging` выводит журнал сервиса в терминал.
`ruststream-fred` - брокер Redis, построенный на клиенте [fred](https://docs.rs/fred).
Его фича `testing` добавляет [Redis внутри процесса](../components/testing/index.md), на котором работают тесты.
`fred` написан на Rust, поэтому сборке не нужны системные библиотеки.

У крейта брокера такие фичи, все выключены по умолчанию:

| Фича | Что она даёт |
|---|---|
| `testing` | Redis внутри процесса, на котором работает `TestApp` |
| `asyncapi` | [расширение Redis](../components/asyncapi/index.md) в генерируемом документе AsyncAPI |
| `tls-rustls`, `tls-rustls-ring`, `tls-native-tls` | [TLS](../components/broker/tls.md) на rustls с `aws-lc-rs`, на rustls с `ring` или на системной библиотеке TLS |
| `sentinel-auth` | отдельные [учётные данные для узлов Sentinel](../components/broker/sentinel-credentials.md) |
| `credential-provider` | [сменяемые учётные данные](../components/broker/credential-provider.md) через колбэк |

## Локальный Redis

Для разработки достаточно одного сервера Redis:

```bash
docker run --name redis -p 6379:6379 redis:8.4
```

Сервер слушает `localhost:6379`.
Redis 8.4 поддерживает все возможности крейта. [Чтению с перехватом](../components/streams/claiming.md) нужна как минимум эта версия.
Valkey работает так же: `RedisBroker::standalone` принимает URL `valkey://`, а для TLS `valkeys://`.

## Первый сервис

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/getting_started.rs#service"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/getting_started.rs#service"
```

</TabItem>
</Tabs>

`take_order` читает заказы в JSON из потока Redis `orders`.
Заказ с суммой он подтверждает, а заказ без суммы отбрасывает.
[`RedisBroker::standalone`](../components/broker/index.md) принимает URL сервера и только запоминает его.
Соединение открывается при запуске сервиса.
Поток Redis читается через группу потребителей.
[`RedisStream::new("orders").group("orders-svc")`](../components/streams/index.md) задаёт поток и группу, а сервис при запуске создаёт их, если их ещё нет.
`#[ruststream::app]` превращает `app` в `main`. В ручной форме то же делает [`run_main`](api:ruststream::runtime::cli::run_main).

Запустите сервис:

```bash
cargo run -- run
```

Добавьте заказ в поток из другого терминала:

```bash
docker exec redis redis-cli XADD orders '*' _payload '{"id":1,"amount_cents":1250}'
```

Полезная нагрузка хранится в поле `_payload` записи.
Сервис пишет в журнал `order taken` с идентификатором заказа.
Новая группа начинает с конца потока, поэтому сервис читает заказы, добавленные после его первого запуска.
<kbd>Ctrl</kbd>+<kbd>C</kbd> останавливает сервис.

`cargo generate` создаёт заготовку такого же сервиса с роутером и ответом.
Для каждой структуры Redis есть свой шаблон: `redis-stream`, `redis-list` и `redis-pubsub`.

```bash
cargo generate --git https://github.com/powersemmi/ruststream-fred templates/redis-stream --name my-service
```

## Первый тест

В файле примера есть ещё два теста.
Каждый запускает то же приложение `app` через `TestApp::start`, публикует заказ и проверяет, как `take_order` завершил его доставку.
Там `RedisBroker` подключается к Redis внутри процесса, поэтому тестам не нужен сервер:

```bash
cargo test
```

Руководство [Тестирование сервиса на Redis](testing.md) разбирает тесты более крупного сервиса и то, что моделирует Redis внутри процесса.

## Переход с fred

Сервис на `fred` сам пишет цикл чтения, подтверждения и создание групп.
Вот где каждая из этих частей находится в RustStream:

| fred | RustStream |
|---|---|
| `Builder::from_config(..)` и `Pool` | [`RedisBroker::standalone`](../components/broker/index.md), [`cluster`](../components/broker/cluster.md) или [`sentinel`](../components/broker/sentinel.md), размер задаёт [`pool`](../components/broker/pool.md); [`from_pool`](../components/broker/from-pool.md) принимает пул, построенный вами |
| `XGROUP CREATE .. MKSTREAM` | выполняет подписка при запуске |
| цикл `XREADGROUP` | обработчик `#[subscriber]` на [`RedisStream`](../components/streams/index.md) |
| декодирование полей записи | тип параметра обработчика, который декодирует кодек |
| `XACK` после работы | [исход](../components/acknowledgement/index.md) обработчика |
| цикл `XAUTOCLAIM` | [`RedisStream::claiming`](../components/streams/claiming.md) или [`reclaim`](../components/streams/reclaim.md) ([Группы потребителей и восстановление после сбоя](consumer-groups.md)) |
| воркер на `BLMOVE` с `LREM` | [`RedisList::reliable`](../components/lists/reliable.md) ([Очереди заданий на списках](work-queues.md)) |
| `SubscriberClient` и его поток сообщений | [`RedisPubSub`](../components/pubsub/index.md) ([Рассылка через Pub/Sub](fan-out.md)) |
| `XADD`, `LPUSH`, `PUBLISH` | ответ или параметр `Out`, отправленный через [политику публикации](../components/publishing/index.md) |
| `MULTI` / `EXEC` вокруг группы `XADD` | [транзакция](../components/transactions/index.md) |
