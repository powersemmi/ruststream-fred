---
sidebar_position: 12
description: "Тестирование сервиса на Redis внутри процесса через TestApp: ответы, собственные команды обработчика, задержки на остановленных часах и тот же тест с сервером."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Тестирование сервиса на Redis

Тест запускает собственное приложение сервиса через [`TestApp`](api:ruststream::testing::TestApp) с [Redis внутри процесса](../components/testing/index.md) или с сервером.

Тестируемый сервис списывает платежи по картам.
Платёж приходит в поток `card-payments`, а обработчик отвечает квитанцией в `card-receipts`.
Платёж без суммы отбрасывается.
Каждый счёт может тратить 1 000,00 в день: обработчик прибавляет сумму к ключу `spent:<account>` через [пул соединений](../components/context/fred-pool.md) брокера и откладывает платёж сверх лимита на минуту через [`retry_after`](../components/acknowledgement/retry-after.md).
После трёх доставок платёж уходит в `card-payments.review`, поток [dead-letter](../components/acknowledgement/dead-letter.md).
Возврат приходит в надёжный список `card-refunds` и возвращает сумму.
Приложение строит `service(url)`, поэтому тест выбирает сервер, а `main` использует промышленный.
Обработчик вызывает пул через трейты команд `fred`, поэтому `fred` входит в зависимости сервиса.

Тестам нужны фича `testing` крейта `ruststream-fred` и рантайм Tokio:

```toml title="Cargo.toml"
[dev-dependencies]
ruststream-fred = { version = "0.7", features = ["testing"] }
tokio = { version = "1", features = ["macros", "rt", "rt-multi-thread"] }
```

## Платёж и его квитанция

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/testing.rs#receipt"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#receipt"
```

</TabItem>
</Tabs>

[`TestApp::start`](api:ruststream::testing::TestApp::start) подключает [`RedisBroker`](../components/broker/index.md) к Redis, смоделированному внутри тестового процесса.
К URL никто не подключается.
Тест обращается к брокеру по его промышленному типу, `tb.broker::<RedisBroker>()`.
Сообщение теста записывается в своё имя так, как этого ждёт подписка, которая читает это имя: `LPUSH` для списка, `PUBLISH` для канала или шаблона, `XADD` для потока и для имени, которое не читает ни одна подписка.
`publish` возвращает управление, когда обработчик и его ответ завершены.
Поэтому следующие строки проверяют готовое состояние: как подписчик завершил доставку и что попало в `card-receipts`.
Тело теста принимает запущенное приложение как аргумент, поэтому его выполняет и тест с сервером ниже.

## Платёж без суммы

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/testing.rs#dropped"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#dropped"
```

</TabItem>
</Tabs>

`settled` проверяет исход, который вернул обработчик.
`assert_not_called` проверяет, что квитанция не опубликована.

## Собственные команды обработчика и задержка

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/testing.rs#held"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#held"
```

</TabItem>
</Tabs>

`INCRBY` и `DECRBY` обработчика выполняются на Redis внутри процесса, как и любая команда крейта.
Второй платёж превышает лимит счёта, поэтому его откладывают.
Возврат - это запись списка, и его обработчик освобождает лимит.

Тест начинается с остановленными часами, а [`advance`](api:ruststream::testing::TestApp::advance) сдвигает их вперёд.
Минута `HOLD` проходит мгновенно.
Redis внутри процесса тоже читает часы теста, поэтому идентификаторы записей, время простоя и оценки во множестве задержек не меняются, пока часы стоят.
`outcomes` перечисляет, как завершилась каждая доставка, по порядку. Отложенная доставка записана как `Outcome::Nack`.

## Предел попыток

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/testing.rs#review"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#review"
```

</TabItem>
</Tabs>

Платёж выше дневного лимита откладывается при каждой доставке.
Каждая отсрочка публикует копию обратно в поток с увеличенным числом попыток, а `published` читает все записи в имя, включая эти копии.
Третья доставка - последняя, которую допускает точка монтирования, поэтому платёж уходит в `card-payments.review` в том виде, в каком пришёл.

## Что моделирует Redis внутри процесса

Redis внутри процесса принимает команды, которые отправляет крейт, и отвечает на них так же, как Redis 8.4, поэтому подписка [с перехватом](../components/streams/claiming.md) монтируется.
Все настройки он читает из брокера: URL, размер пула, группу по умолчанию и топологию кластера.
Он хранит:

- типы ключей, поэтому запись неверного типа возвращает ошибку `WRONGTYPE`;
- группы потребителей с курсором, неподтверждёнными записями и счётчиками доставок, `XAUTOCLAIM`, `XREADGROUP .. CLAIM` и [перемотку](../components/positions/seek.md);
- списки вместе со списком обработки и множеством для восстановления надёжного списка;
- каналы Pub/Sub, шаблоны и шардированные каналы, которые ничего не хранят для отсутствующего подписчика;
- упорядоченные множества, срок жизни ключей и `MULTI` / `EXEC`;
- в топологии кластера отказ выполнять команду или транзакцию, которая охватывает несколько хеш-слотов.

Собственные команды обработчика, через пул или в [конвейерном](../components/pipelining/index.md) раунде, тоже выполняются на нём.
Скрипты он не выполняет: `EVAL`, `EVALSHA`, `FCALL` и незнакомая ему команда возвращают ошибку, которая их называет.
Рукопожатие не выполняется, поэтому аутентификация и TLS не проверяются, как и переключение при отказе и перераспределение слотов.
Для них нужен сервер.

## Тот же тест с сервером

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/testing.rs#live"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#live"
```

</TabItem>
</Tabs>

[`TestApp::start_live`](../components/testing/live.md) подключает то же приложение к работающему Redis.
Тогда публикация ждёт по настоящим часам, пока её не обработает каждая подписка, но не больше десяти секунд.
Тест читает адрес из `REDIS_TEST_URL` через `live::url`, собственную вспомогательную функцию примера, и пропускается, если переменная не задана.
Сервер хранит то, что оставили предыдущие запуски, поэтому тест тратит деньги с собственного счёта.
Проверки те же, что и внутри процесса, без изменений.

```bash
cargo test
docker run --name redis -p 6379:6379 redis:8.4
REDIS_TEST_URL=redis://localhost:6379 cargo test -- --test-threads=1
```
