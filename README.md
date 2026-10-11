# Kafka, задание 1

## Запуск

Для Docker VM желательно хотя бы 4 GiB памяти.
Из `assignments/module1`:

```bash
docker-compose up --build -d
```

Топик создаётся автоматически после готовности брокеров. `kafka-init` должен
завершиться с кодом 0, остальные сервисы — работать. UI: http://localhost:8080.
Все следующие команды выполняются из той же папки.

## Что делает код

Файлы приложения находятся в `src/main/kotlin/module1`:

- `Producer.kt` — создаёт заказ с UUID, отправляет Protobuf и печатает ID,
  партицию и offset. После подтверждения ждёт секунду. Настроены `acks=all`,
  idempotence и retries с бюджетом доставки 60 секунд. При окончательной ошибке
  логирует её и завершается; хранения события для повторной отправки нет.
- `SingleMessageConsumer.kt` — не больше одной записи за `poll()`, группа
  `single`, автокоммит каждые 5 секунд. В Compose запущены две реплики.
- `BatchConsumer.kt` — группа `batch`, накапливает минимум 10 записей,
  обрабатывает в цикле и вызывает `commitSync()` один раз после пачки.
- `Topics.kt` — имя топика. Модель — `src/main/proto/order_created.proto`;
  Maven генерирует `OrderCreated`. Schema Registry не нужен.

Группы читают поток независимо, реплики внутри группы делят партиции.
У обеих групп `earliest`: без сохранённого offset читают доступную историю,
с сохранённым — продолжают с него. Ошибки декодирования и обработки записи
логируются, запись пропускается.

## Проверка

```bash
docker-compose ps -a
docker-compose logs -f --tail=30 producer single-message-consumer batch-consumer
```

Найти одинаковый `orderId` в `Produced` и `Consumed` обеих групп.
Batch выводит размер каждого `poll()`, накопленного буфера и обработанной пачки.
Нужно дождаться хотя бы 10 сообщений.

```bash
docker-compose exec -T kafka /usr/bin/kafka-topics --bootstrap-server kafka:29092 --describe --topic orders.created.protobuf
docker-compose exec -T kafka /usr/bin/kafka-consumer-groups --bootstrap-server kafka:29092 --describe --all-groups
```

Для проверки ошибки добавить одну некорректную запись, не останавливая продюсер:

```bash
printf 'not-protobuf\n' | docker-compose exec -T kafka /usr/bin/kafka-console-producer \
  --bootstrap-server kafka:29092 --topic orders.created.protobuf
```

В логах обеих групп появится `Protobuf decoding failed`, затем обработка новых
сообщений продолжится. Batch покажет ошибку после накопления пачки.

Важно: минимум 10 гарантируется для обработки, не для одного `poll()`.
`fetch.min.bytes` и `fetch.max.wait.ms` управляют байтами и ожиданием, а не
минимальным количеством записей. Поэтому используется аккумулятор.

Остановка: `docker-compose down`. Данные остаются в томах; `down -v` их удалит.
Сборка без Docker: JDK 21, Maven, команда `mvn clean package` из корня проекта.
