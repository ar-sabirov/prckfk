# Kafka, модуль 2: блокировки, цензура и статистика

## Логика и код

Групповой чат: каждое сообщение проходит цензуру и отправляется всем участникам,
кроме отправителя и заблокировавших его пользователей. В `filtered_messages`
попадает один `DeliveryEnvelope` с текстом и списком получателей.
Модели — [chat.proto](../../src/main/proto/chat.proto), код — [module2](../../src/main/kotlin/module2):

- `ChatApplication.kt`, `ChatConfig.kt` — запуск и настройки Kafka Streams;
  `ChatTopicSetup.kt`, `ChatTopics.kt` — создание и проверка топиков.
- `ChatTopology.kt` — `messages → цензура → выбор получателей → filtered_messages`;
  справочники `users` и `stop_words` — глобальные таблицы.
- `BlockCommandProcessor.kt` — применяет команды из `blocked_users` к индексу
  `blockedBy[отправитель] = заблокировавшие его получатели`.
- `CensorshipProcessor.kt`, `Censorship.kt` — заменяют запрещённые слова на `***`
  без учёта регистра, сохраняя пунктуацию: `BAD!` → `***!`, `badge` → `badge`.
- `RecipientSelectionProcessor.kt` — вычисляет `все участники − отправитель − blockedBy[отправитель]`.
- `ChatCommands.kt` — публикует сообщения, изменения участников, блокировок и стоп-слов;
  удаление участника или слова — запись с `null`. `ChatConsumer.kt` выводит результат в лог.

Блокировки индивидуальны, но хранятся обратным индексом в персистентном
`chat-blocked-by`, с Kafka changelog и Docker-томом `chat-state`.
`messages` и `blocked_users` используют ключ отправителя и по 3 партиции;
общая подтопология размещает сообщения, команды и состояние в одной Streams-задаче.
Между топиками порядка нет: блокировка действует после применения команды.

## Запуск

Нужны Docker с Compose; для Docker VM желательно от 4 GiB памяти.
Все команды выполняются из `assignments/module2`. Для точных результатов теста
нужно пустое состояние: `docker-compose down -v --remove-orphans` **удаляет данные стенда**.

```bash
docker-compose up --build -d
docker-compose ps -a
docker-compose logs --tail=50 demo-init ksqldb-init chat
```

Compose создаёт топики, запускает Kafka, ksqlDB, `chat` и `consumer`.
`demo-init` добавляет Alice, Bob, Степана, стоп-слово `bad` и блокировку Alice → Bob;
`ksqldb-init` выполняет [ksqldb-queries.sql](ksqldb/ksqldb-queries.sql).
Дождитесь `healthy` у Kafka и ksqlDB, кода 0 у трёх init-сервисов,
а в логах `chat` — `RUNNING` и `Applied BLOCK recipient=Alice sender=Bob`.

Повторный `up` сохраняет правила и SQL-объекты; существующие определения SQL не обновляются.
[Kafka UI](http://localhost:18080) позволяет смотреть топики и выполнять SQL.

## Проверка блокировки и цензуры

Команды `send` публикуют тестовые данные в `messages`; результат читает `consumer`:

```bash
docker-compose run --rm --no-deps commands send Bob m1 'This is BAD!'
docker-compose run --rm --no-deps commands send Bob m2 'bad badge'
docker-compose run --rm --no-deps commands send Степан m3 'Привет!'
docker-compose logs --tail=30 consumer
```

| ID | Отправитель | Текст на выходе | Получатели |
| --- | --- | --- | --- |
| `m1` | Bob | `This is ***!` | Степан |
| `m2` | Bob | `*** badge` | Степан |
| `m3` | Степан | `Привет!` | Alice, Bob |

Дождитесь всех трёх записей; порядок получателей не важен.

### Сохранение блокировки после перезапуска

```bash
docker-compose restart chat
docker-compose logs --since=1m --tail=30 chat
```

Дождитесь нового `RUNNING`, затем отправьте сообщение без повторной публикации правил:

```bash
docker-compose run --rm --no-deps commands send Bob m4 'BAD after restart'
docker-compose logs --tail=30 consumer
```

У `m4` ожидаются текст `*** after restart` и только Степан в получателях.

Теперь снимите блокировку:

```bash
docker-compose run --rm --no-deps commands unblock Alice Bob
docker-compose logs --tail=30 chat
```

Дождитесь `Applied UNBLOCK recipient=Alice sender=Bob`, затем отправьте:

```bash
docker-compose run --rm --no-deps commands send Bob m5 'bad badge'
docker-compose logs --tail=30 consumer
```

У `m5` текст `*** badge`, получатели — Alice и Степан.

## Проверка статистики

[SQL](ksqldb/ksqldb-queries.sql) считает сообщения и уникальных получателей по отправителям
(`USER_STATISTICS`, топик `user_statistics`) и глобально (`GLOBAL_STATISTICS`, `global_statistics`).
Уникальность определяется по таблицам пар «отправитель — получатель» и всех получателей.
Сообщение считается один раз, даже при пустом списке получателей.

После нужного этапа выполните [test-queries.sql](ksqldb/test-queries.sql):

```bash
docker-compose run --rm --no-deps ksqldb-check
```

Он выполняет следующие запросы (также доступны в Kafka UI):

```sql
SHOW QUERIES;
SELECT * FROM USER_STATISTICS;
SELECT * FROM GLOBAL_STATISTICS WHERE STATISTICS_KEY = 'all';
```

Ожидаются 9 работающих постоянных запросов. Дождитесь обновления таблиц:

| После шага | Bob: сообщения / уникальные получатели | Степан: сообщения / уникальные получатели | Глобально: сообщения / уникальные получатели |
| --- | --- | --- | --- |
| `m1`–`m3` | 2 / 1 | 1 / 2 | 3 / 3 |
| `m4` | 3 / 1 | 1 / 2 | 4 / 3 |
| `m5` | 4 / 2 | 1 / 2 | 5 / 3 |

У Alice нет строки: она не отправляла сообщения. Повторный `send` увеличивает счётчики
даже с прежним ID; блокировка или удаление участника не уменьшает историческую статистику.

## Изменение стоп-слов

После основного сценария можно добавить и удалить слово без перезапуска:

```bash
docker-compose run --rm --no-deps commands add-stop-word evil
docker-compose run --rm --no-deps commands remove-stop-word evil
```

После применения добавления `evil` маскируется в новых сообщениях, после удаления — сохраняется.
Динамические обновления, блокировки и цензуру проверяют [автотесты](../../src/test/kotlin/module2),
которые запускаются при сборке Docker-образа (`mvn verify`).

Остановка: `docker-compose down`. Данные остаются в томах; `down -v` их удалит.
