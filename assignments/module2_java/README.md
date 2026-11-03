# Kafka, модуль 2 на Java: блокировки, цензура и статистика

Это полная Java 21-версия приложения из `module2`. Исходная Kotlin-версия
остаётся доступна отдельно. Java-код использует общие с ней сгенерированные
классы `module2.proto` из `chat.proto`; бизнес-логика реализована в `module2_java`.
ksqlDB-запросы сохранены в SQL, так как выполняются самим сервером ksqlDB.

## Логика и код

Групповой чат: каждое сообщение проходит цензуру и отправляется всем участникам,
кроме отправителя и заблокировавших его пользователей. В `filtered_messages`
попадает один `DeliveryEnvelope` с текстом и списком получателей.
Модели — [chat.proto](../../src/main/proto/chat.proto), код — [module2_java](../../src/main/java/module2_java):

- `ChatApplication.java`, `ChatConfig.java` — запуск и настройки Kafka Streams;
  `ChatTopicSetup.java`, `ChatTopics.java` — создание и проверка топиков.
- `ChatTopology.java` — `messages → цензура → выбор получателей → filtered_messages`;
  справочники `users` и `stop_words` — глобальные таблицы.
- `BlockCommandProcessor.java` — применяет команды из `blocked_users` к индексу
  `blockedBy[отправитель] = заблокировавшие его получатели`.
- `CensorshipProcessor.java`, `Censorship.java` — заменяют запрещённые слова на `***`
  без учёта регистра, сохраняя пунктуацию: `BAD!` → `***!`, `badge` → `badge`.
- `RecipientSelectionProcessor.java` — вычисляет `все участники − отправитель − blockedBy[отправитель]`.
- `ChatCommands.java` — публикует сообщения, изменения участников, блокировок и стоп-слов;
  удаление участника или слова — запись с `null`. `ChatConsumer.java` выводит результат в лог.

Блокировки индивидуальны, но хранятся обратным индексом в персистентном
`chat-blocked-by`, с Kafka changelog и Docker-томом `chat-state`.
`messages` и `blocked_users` используют ключ отправителя и по 3 партиции;
общая подтопология размещает сообщения, команды и состояние в одной Streams-задаче.
Между топиками порядка нет: блокировка действует после применения команды.

## Запуск

Нужны Docker с Compose; для Docker VM желательно от 4 GiB памяти.
Все команды выполняются из `assignments/module2_java`. Для точных результатов теста
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
[Kafka UI](http://localhost:18081) позволяет смотреть топики и выполнять SQL.

Java-стенд использует отдельные Compose-проект, тома и порты:
Kafka — `localhost:19093`, ksqlDB — `http://localhost:18089`,
Kafka UI — `http://localhost:18081`.

### Сборка и запуск без контейнера приложения

Из корня репозитория, с JDK 21 и Maven:

```bash
mvn verify
java -cp target/app.jar module2_java.ChatCommands help
```

Сборка компилирует обе реализации и запускает Java- и Kotlin-тесты.
Только Java-тесты: `mvn -Dtest='module2_java.*Test' test`.

При отдельном запуске Kafka (без сервиса `chat`) точками входа служат:

```bash
java -cp target/app.jar module2_java.ChatTopicSetup
java -cp target/app.jar module2_java.ChatApplication
# В другом терминале:
java -cp target/app.jar module2_java.ChatConsumer
# В третьем терминале:
java -cp target/app.jar module2_java.ChatCommands add Alice Bob Степан
java -cp target/app.jar module2_java.ChatCommands send Bob m1 'This is BAD!'
```

Настройки задаются переменными окружения:

| Переменная | По умолчанию |
| --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:19093` |
| `KAFKA_APPLICATION_ID` | `module2-java-chat` |
| `KAFKA_STATE_DIR` | `target/module2-java-state` |
| `KAFKA_CONSUMER_GROUP_ID` | `module2-java-chat-logger` |

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
Динамические обновления, блокировки и цензуру проверяют [автотесты](../../src/test/java/module2_java),
которые запускаются при сборке Docker-образа (`mvn verify`).

Остановка: `docker-compose down`. Данные остаются в томах; `down -v` их удалит.
