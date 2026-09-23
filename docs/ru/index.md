# Брокер Redis {#redis-broker}

`ruststream-fred` запускает сервис [RustStream](https://powersemmi.github.io/ruststream/) на Redis.
Redis Streams - это журнал, как Kafka: подписка читает его через группу потребителей и подтверждает
каждую обработанную запись. Списки и Pub/Sub тоже здесь, а фича `testing` поставляет
внутрипроцессный тестовый брокер, поэтому тесты идут без сервера Redis.

```toml
ruststream = { version = ">=0.7.0-rc.9, <0.8.0", features = ["macros"] }
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
`.message(&value).publish()` и больше ничего; исключение - ключ партиционирования, шаг этого
билдера. Тело, которое его задаёт, импортирует прелюдию этого крейта и называет тип настроек в своём
ограничении; см.
[ключи партиционирования](https://docs.rs/ruststream-fred/latest/ruststream_fred/index.html#partition-keys).

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

```rust
--8<-- "crates/ruststream-fred/examples/fred_topologies.rs:topologies"
```

## Где всё остальное {#where-the-rest-is}

Справочник на docs.rs открывается учебником по самому крейту, по разделу на тему:

- [Подписка](https://docs.rs/ruststream-fred/latest/ruststream_fred/index.html#subscribing):
  дескрипторы и ответ каждого из них о повторной доставке, по транспортам
  ([потоки](https://docs.rs/ruststream-fred/latest/ruststream_fred/index.html#streams),
  [списки](https://docs.rs/ruststream-fred/latest/ruststream_fred/index.html#lists),
  [Pub/Sub](https://docs.rs/ruststream-fred/latest/ruststream_fred/index.html#pubsub)), а также
  [пакеты](https://docs.rs/ruststream-fred/latest/ruststream_fred/index.html#batches),
  [нативные поля доставки](https://docs.rs/ruststream-fred/latest/ruststream_fred/index.html#native-delivery-fields),
  [отложенная повторная доставка](https://docs.rs/ruststream-fred/latest/ruststream_fred/index.html#delayed-retry),
  [предел доставок](https://docs.rs/ruststream-fred/latest/ruststream_fred/index.html#capping-the-retries)
  и
  [перемотка группы](https://docs.rs/ruststream-fred/latest/ruststream_fred/index.html#repositioning-a-group).
- [Публикация](https://docs.rs/ruststream-fred/latest/ruststream_fred/index.html#publishing):
  политики, шаг
  [ключа партиционирования](https://docs.rs/ruststream-fred/latest/ruststream_fred/index.html#partition-keys)
  и
  [транзакции](https://docs.rs/ruststream-fred/latest/ruststream_fred/index.html#transactions).
- [Сгенерированный документ](https://docs.rs/ruststream-fred/latest/ruststream_fred/index.html#the-generated-document):
  что документ AsyncAPI говорит о канале Redis и что в него намеренно не попадает.
- [Тестирование](https://docs.rs/ruststream-fred/latest/ruststream_fred/testing/index.html):
  внутрипроцессный транспорт, что он воспроизводит и что стоит проверять на настоящем сервере.
- [Эксплуатация](https://docs.rs/ruststream-fred/latest/ruststream_fred/index.html#operations):
  топологии, учётные данные, TLS и известные ограничения.

Обработчики, роутеры, кодеки и middleware даёт сам фреймворк, а его входные страницы начинаются
с [сайта RustStream](https://powersemmi.github.io/ruststream/).
