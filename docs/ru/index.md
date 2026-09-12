# Брокер Redis {#redis-broker}

`ruststream-fred` запускает сервис [RustStream](https://powersemmi.github.io/ruststream/) на Redis.
Redis Streams - это журнал, как Kafka: подписка читает его через группу потребителей и подтверждает
каждую обработанную запись. Списки и Pub/Sub тоже здесь, а фича `testing` поставляет
внутрипроцессный тестовый брокер, поэтому тесты идут без сервера Redis.

```toml
ruststream = { version = "0.7", features = ["macros"] }
ruststream-fred = "0.7"
serde = { version = "1", features = ["derive"] }
```

`RedisBroker::standalone` синхронный и не делает ввода-вывода: соединение рантайм открывает при
старте и закрывает при остановке.

Политику публикации вы указываете при регистрации обработчика: `RedisPublish` для потоков,
`RedisPubSubPublish` для каналов, `RedisListPublish` для списков. Издателя по этой политике рантайм
инстанцирует на подключённом брокере, поэтому публикация до подключения непредставима.

Политика покрывает публикацию целиком, кроме одной настройки самого сообщения. `XADD`, `LPUSH` и
`PUBLISH` принимают ключ или канал и значение, поэтому тело обработчика обычно пишет
`.message(&value).publish()` и больше ничего; исключение - ключ раздела, шаг этого билдера. Тело,
которое его задаёт, импортирует прелюдию этого крейта и называет тип настроек в своём ограничении;
см. [ключи разделов](streams.md#partition-keys).

## Заготовка сервиса {#scaffold-a-service}

Рабочую заготовку сгенерирует [`cargo generate`](https://github.com/cargo-generate/cargo-generate),
по шаблону на каждый транспорт:

```bash
cargo generate --git https://github.com/powersemmi/ruststream-fred templates/redis-stream
cargo generate --git https://github.com/powersemmi/ruststream-fred templates/redis-pubsub
cargo generate --git https://github.com/powersemmi/ruststream-fred templates/redis-list
```

## Топологии {#topologies}

Топологию выбирает один из трёх именованных конструкторов:

```toml
# standalone
# RedisBroker::standalone("redis://localhost:6379")
# cluster (one reachable seed node is enough; the rest is discovered)
# RedisBroker::cluster(["127.0.0.1:7000", "127.0.0.1:7001"])
# sentinel (the monitored primary's name plus the sentinels)
# RedisBroker::sentinel("mymaster", ["127.0.0.1:26379"])
```

## Руководства по транспортам {#transport-guides}

- [Redis Streams](streams.md) - группы потребителей, чтение с конца против перехвата, пакеты,
  перемотка, отложенная повторная доставка.
- [Списки Redis](lists.md) - очередь заданий с конкурирующими потребителями, надёжный режим,
  восстановление осиротевших записей.
- [Pub/Sub](pubsub.md) - классическая и шардированная рассылка.
- [Dead-letter и предел доставок](dead-letter.md) - как ограничить бесконечную повторную доставку.
- [Аутентификация и TLS](auth-tls.md) - учётные данные и TLS на любой топологии.
- [Транзакции](transactions.md) - пакетная публикация на standalone и sentinel.
- [Тестирование](testing.md) - запуск сервиса и его обработчиков в процессе, без сервера Redis.
