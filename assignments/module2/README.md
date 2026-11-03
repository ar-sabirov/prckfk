# Module 2: Kafka Streams chat

## Goal

Implement one shared group chat using Kafka Streams. Users can block messages
from other users. A global stop-word list controls censorship, and the application
maintains global and per-sender statistics.

There is no `groupId`: all registered users belong to the same chat. Censorship,
recipient selection, blocking, and statistics run inside Kafka Streams.

## Chat behavior

### Recipients and blocking

- A message targets all registered users except its sender.
- Blocking is directional: if Alice blocks Bob, Alice does not receive Bob's
  messages. Bob can still receive Alice's messages.
- Blocking affects only the user who created the block. Other eligible users
  still receive the message.
- Unblocking restores eligibility for subsequent messages.

The output is a delivery instruction containing the message and its eligible
recipient IDs. It may be split into batches when necessary. A delivery consumer
follows those instructions without evaluating blocking or censorship again.

### Censorship

Replace each whole-word occurrence of a configured stop word with exactly `***`.
Matching is case-insensitive; surrounding text and punctuation remain unchanged.
The same stop-word list applies to every user.

For example, with `bad` configured as a stop word:

| Original text | Censored text |
| --- | --- |
| `This is BAD!` | `This is ***!` |
| `bad, really bad!` | `***, really ***!` |
| `badge` | `badge` |
| `bad` | `***` |

Censorship transforms the text; it does not reject the message. A message whose
entire text becomes `***` remains eligible for delivery and counts normally in
statistics. Use the term **censorship** in code, for example
`CensorshipProcessor` or `censor(text, stopWords)`.

### Rule updates

A message uses the rule state available when it is processed. Updates affect
subsequent processing; they do not retract earlier delivery instructions or
recalculate historical statistics. Publishing an update does not by itself
confirm that processing tasks have applied it.

## Processing flow

```text
Message -> Censorship -> Recipient selection and blocking
                                      |
                         +------------+------------+
                         |                         |
                  Delivery output              Statistics
```

Statistics use the final recipient selection. Count each chat message once,
regardless of its number of recipients, masked words, or output batches. A
message with no eligible recipients still contributes one to the message count.

## Statistics

Per-user statistics describe the user as a **sender**. Recipient counts describe
users selected for delivery, not acknowledgements that clients received the
message. All statistics are cumulative over the retained application history.

| Metric | Definition |
| --- | --- |
| Global message count | Number of processed chat messages, counted once per message. |
| Global unique recipient count | Number of distinct users selected for at least one message from any sender. |
| Per-user message count | Number of processed chat messages sent by that user. |
| Per-user unique recipient count | Number of distinct users selected for at least one message from that sender. |

Unique recipient counts are exact. Repeated messages to the same recipient do
not increase the corresponding unique count. Global uniqueness is calculated
across all senders; it is not the sum of their individual unique counts.
Blocking or unblocking someone does not remove them from historical recipient
counts. User and rule updates are not chat messages and do not increase message
counts.

## Example scenario

Register Alice, Bob, and Stepan. Configure `bad` as a stop word and have Alice
block Bob. Apply these updates before processing the following messages.

| Step | Action | Delivered text | Eligible recipients | Global messages | Global unique recipients |
| --- | --- | --- | --- | --- | --- |
| 1 | Bob sends `This is BAD!` | `This is ***!` | Stepan | 1 | 1 |
| 2 | Bob sends `Hello again` | `Hello again` | Stepan | 2 | 1 |
| 3 | Stepan sends `Hello` | `Hello` | Alice, Bob | 3 | 3 |
| 4 | Alice unblocks Bob; apply the update | — | — | 3 | 3 |
| 5 | Bob sends `bad badge` | `*** badge` | Alice, Stepan | 4 | 3 |

Final per-user statistics:

| Sender | Messages | Unique recipients | Recipients reached |
| --- | --- | --- | --- |
| Alice | 0 | 0 | None |
| Bob | 3 | 2 | Alice, Stepan |
| Stepan | 1 | 2 | Alice, Bob |

Bob and Stepan each reached two distinct recipients, but the global unique
recipient count is three because Alice appears in both sets.

## Events, topics, and keys

The wire models are defined in [chat.proto](../../src/main/proto/chat.proto).
Maven generates their Java classes in the `module2.proto` package for use
from the JVM application. Topic names and partition counts are defined in
[ChatTopics.kt](../../src/main/kotlin/module2/ChatTopics.kt).

All Kafka keys are non-null UTF-8 strings, serialized with `StringSerializer`.
Non-null values are Protobuf bytes. The examples below use Protobuf text format
for readability; they are not JSON records or commands to run.

### Topic contracts

| Topic | Key | Value | Partitions | Cleanup policy |
| --- | --- | --- | --- | --- |
| `module2.chat.messages` | `sender_id` | `ChatMessage` | 3 | `delete` |
| `module2.chat.users` | `user_id` | `User`, or a tombstone to remove membership | 1 | `compact` |
| `module2.chat.block-commands` | `blocked_sender_id` | `BlockCommand` | 3 | `delete`; retain the command history for this exercise (`retention.ms=-1`) |
| `module2.chat.stop-words` | Normalized `word` | `StopWord`, or a tombstone to remove the word | 1 | `compact` |
| `module2.chat.deliveries` | `message.sender_id` | `DeliveryEnvelope` | 3 | `delete` |

The topic setup creates these topics and validates the partition counts and
cleanup settings of existing topics. The models and constants themselves do not
create topics. Statistics contracts will be added with steps 7 and 8.
Three sender partitions are a small demonstration choice that allows work to be
split across multiple tasks. This number is independent of the number of users;
many senders can share one partition.

Messages and block commands are event logs. In particular, do not compact block
commands by sender ID: Alice's block of Bob and Stepan's block of Bob are separate
commands with the same key. Keeping only the latest command would lose part of
the blocking state when rebuilding it. The application will aggregate commands
into its inverse blocking index in step 5.

Users and stop words describe current state. A non-null record creates or
replaces the entry for its key. A tombstone is a Kafka record with a key and a
null value; it deletes that entry. It is not an empty Protobuf message or the
string `"null"`. Compaction can discard superseded values while retaining the
state needed by a table. Removing a user does not erase historical statistics.

### Model rules

- `ChatMessage` contains `message_id`, `sender_id`, and `text`. The Kafka key must
  equal `sender_id`. Preserve the message ID through censorship and delivery.
  An ID identifies the logical message; the field alone does not implement
  deduplication of repeated submissions.
- `User` contains `user_id`, which must equal its Kafka key. Repeating the same
  entry does not create a second member.
- `BlockCommand` contains `blocked_sender_id`, `recipient_id`, and an action:
  `BLOCK` or `UNBLOCK`. The recipient is the user choosing whom to block.
  `ACTION_UNSPECIFIED` is not a valid command. Repeated `BLOCK` and `UNBLOCK`
  commands have set semantics: adding an existing member or removing an absent
  member leaves the set unchanged. Use `UNBLOCK`, not a tombstone, to unblock.
- `StopWord` contains one non-empty word. Normalize the entry by trimming
  surrounding whitespace and lowercasing with `Locale.ROOT`; use the resulting
  word as both key and value field. For example, an entry of `BAD` is stored
  under `bad`. Censorship will apply the matching normalization to message words
  while preserving the surrounding original text.
- `DeliveryEnvelope` contains the message with censored text and distinct
  `recipient_ids` excluding the sender. Its Kafka key remains the sender ID,
  not a recipient ID. Initially, emit one envelope per message, including an
  empty recipient list when nobody is eligible. No delivery consumer sends an
  empty list to anyone, but the original message still counts in statistics.

IDs are non-empty, case-sensitive strings. Fields duplicating Kafka keys make
payloads self-contained; producers must keep them consistent. Protobuf describes
the wire format but does not enforce these business rules. Runtime validation
will be added alongside the processors that consume each model.

### Partitioning checkpoint

Trace the two related inputs:

```text
Alice blocks Bob:
  topic: module2.chat.block-commands
  key:   bob
  value: blocked_sender_id: "bob" recipient_id: "alice" action: BLOCK

Bob sends a message:
  topic: module2.chat.messages
  key:   bob
  value: message_id: "m-1" sender_id: "bob" text: "This is BAD!"
```

Both keys serialize to the same bytes. With three partitions on both topics
and the same default keyed partitioning, both records map to the same partition
number in their respective topics. The topology must connect these inputs so
Kafka Streams assigns the corresponding partitions and blocking state to the
same task. Matching topic keys alone does not connect independent topologies.

That task can maintain `blockedBy["bob"] = {"alice"}` and look it up locally for
Bob's messages. His per-sender statistics can also be maintained in that task.
The user registry is replicated to every Streams instance; the stop-word table
will follow in step 6. Their partition counts do not need to match the
sender-partitioned inputs.

After the block and the stop word `bad` are applied, with Alice, Bob, and Stepan
registered, the eventual delivery record is:

```text
topic: module2.chat.deliveries
key:   bob
value:
  message { message_id: "m-1" sender_id: "bob" text: "This is ***!" }
  recipient_ids: "stepan"
```

This is the expected output once steps 4–6 are implemented. Equal partition
numbers do not establish ordering between the message and block-command topics;
the block must have been applied before processing the message for this example
to hold. Do not explicitly choose different partitions for these related keys
or change one input topic's partition count independently.

### Verify the contracts

With JDK 21 and Maven available, run from the repository root:

```bash
mvn -Dkotlin.compiler.daemon=false compile
```

This generates and compiles the Protobuf classes together with the application
sources. No Kafka broker is needed for this check. The daemon flag runs the
Kotlin compiler in the Maven process. Generated sources live under
`target/generated-sources/protobuf`; edit the `.proto` definitions rather than
those generated files.

## Running step 4

The current topology selects recipients from the user registry:

```text
module2.chat.users -> GlobalKTable (local copy on each Streams instance)
                                      |
producer -> module2.chat.messages -> chat (Kafka Streams) -> module2.chat.deliveries -> consumer
```

It parses each `ChatMessage` and wraps it in a `DeliveryEnvelope`, preserving
the sender key and the message fields. The recipients are all currently
registered users except the sender. Blocking, censorship, and statistics follow
in later steps; the stop-word topic is not consumed yet.

### User registry and recipient selection

`ChatTopology` materializes `module2.chat.users` as a `GlobalKTable` in the local
store named `chat-users`. Every Streams instance consumes all partitions of the
user topic and maintains its own complete copy. This lets any message task
select recipients locally, regardless of which sender partitions it owns.

`RecipientSelectionProcessor` reads that store for each message, excludes the
sender, and forwards one envelope. A normal table join looks up a value for one
key; recipient selection needs to enumerate the membership instead. The fixed-key
processor keeps the sender key, so this operation requires no repartitioning.

Registering the same ID again replaces its entry; it does not create a duplicate
recipient. A tombstone removes the entry. A user can receive messages without
having sent any. An empty registry, or one containing only the sender, still
produces one envelope with `recipients=[]`. Recipient order is not a contract.

Selection scans the registry and includes the recipient IDs in the envelope, so
its work and output size grow with membership. It avoids producing a separate
Kafka record per recipient; it does not eliminate the cost of listing recipients.

Message processing validates non-empty message IDs and keys, equality between
the sender ID and key, and non-empty user IDs matching their registry keys.
Protobuf payloads must be valid. Invalid input fails processing rather than
silently generating an incorrect delivery; user payloads are checked when the
processor reads the registry.

### Files and responsibilities

Application sources use package `module2` and live directly in
`src/main/kotlin/module2`. Generated Protobuf classes use `module2.proto`.

| File | Responsibility |
| --- | --- |
| [ChatTopology.kt](../../src/main/kotlin/module2/ChatTopology.kt) | Build the global user table and connect message input, recipient selection, and delivery output. |
| [RecipientSelectionProcessor.kt](../../src/main/kotlin/module2/RecipientSelectionProcessor.kt) | Read local membership, exclude the sender, and emit one delivery envelope. |
| [ChatConfig.kt](../../src/main/kotlin/module2/ChatConfig.kt) | Configure the broker connection, application ID, stream thread, offsets, and state directory. |
| [ChatApplication.kt](../../src/main/kotlin/module2/ChatApplication.kt) | Print the topology, start Kafka Streams, report state transitions, and close on shutdown. |
| [ChatTopicSetup.kt](../../src/main/kotlin/module2/ChatTopicSetup.kt) | Create and check topics, then seed demo users; accept `add`/`remove` commands for membership changes. |
| [ChatProducer.kt](../../src/main/kotlin/module2/ChatProducer.kt) | Continuously send sentences as Alice, Bob, and Stepan in round-robin order. |
| [SentenceSource.kt](../../src/main/kotlin/module2/SentenceSource.kt) | Download plain text once and split it into sentences. |
| [ChatConsumer.kt](../../src/main/kotlin/module2/ChatConsumer.kt) | Subscribe to deliveries, decode each envelope, and log its message and recipients. |
| [docker-compose.yml](docker-compose.yml) | Run Kafka, topic and user setup, and separate producer, Streams, and consumer processes. |
| [ChatTopologyTest.kt](../../src/test/kotlin/module2/ChatTopologyTest.kt) | Test membership changes, recipient selection, and validation without a broker. |

The local broker uses KRaft with broker and controller roles in one process and
replication factor 1. It exposes `localhost:19092` for host clients; containers
connect to `kafka:29092`. Port 19092 keeps this exercise separate from module 1's
host ports. The application reuses the repository's Maven/JDK 21 Docker build.

### Start and verify

Start your container runtime first (`colima start` if using Colima). Run these
commands from `assignments/module2`:

```bash
docker-compose up --build -d
docker-compose ps -a
docker-compose logs --tail=50 kafka-init chat
docker-compose logs -f --tail=10 producer consumer
```

The broker must become healthy; `kafka-init` creates and checks the topics, then
registers Alice, Bob, and Stepan and exits with code 0. Only then does `chat`
start. Kafka Streams restores the global user table before starting message
processing, so the demo's initial membership is available without an arbitrary
sleep. `chat` must reach `RUNNING`.

The `producer` and `consumer` services keep running until stopped. Each uses its
own Kafka client; neither runs inside the Streams process.
The producer can start sending while Streams is joining its consumer group;
Kafka retains those messages until Streams processes them. After a restart,
group rebalancing may delay the first output.
The logs have this form (text abbreviated here):

```text
Produced topic=module2.chat.messages partition=<n> offset=<input-offset> key=alice messageId=<id> text="..."
Consumed topic=module2.chat.deliveries partition=<n> offset=<output-offset> key=alice messageId=<id> recipients=[bob, stepan] text="..."
```

Compare the same `messageId` in both logs to see its journey through the app.
The consumer prints every record; it does not filter for a particular ID or
perform assertions. With the three demo users registered, each message has the
other two users as recipients. Text is unchanged at this step. Censorship starts
in step 6.

### Change membership while the demo runs

Run these commands from `assignments/module2`:

```bash
docker-compose run --rm --no-deps kafka-init module2.ChatTopicSetupKt remove stepan
docker-compose logs -f --tail=10 consumer
```

Once the tombstone has been applied, Bob's messages target only Alice. The
producer continues cycling through its three sender IDs; membership controls
recipient selection. Restore Stepan with:

```bash
docker-compose run --rm --no-deps kafka-init module2.ChatTopicSetupKt add stepan
```

You can also register a user who never sends a message, then remove them:

```bash
docker-compose run --rm --no-deps kafka-init module2.ChatTopicSetupKt add dana
docker-compose run --rm --no-deps kafka-init module2.ChatTopicSetupKt remove dana
```

The command waits for Kafka to acknowledge the update. The global table consumes
updates asynchronously, so that acknowledgement does not mean every Streams
instance has applied it yet. Observe subsequent delivery logs to see the change.
Membership changes do not emit deliveries or modify earlier envelopes. Tests
apply user updates before the corresponding messages explicitly.

Running `ChatTopicSetupKt` without arguments creates/checks topics and registers
the three demo users. The `add` and `remove` commands only update the specified
users; they do not run the default setup. Each Compose startup that runs
`kafka-init` re-registers the demo users. Other memberships persist in the
compacted topic until removed with tombstones.

### Sentence source and sender rotation

The default source is the Russian text of *War and Peace* from
[this gist](https://gist.github.com/romaklimenko/c95f3a864828f7f034b7a33d1676e420).
The producer downloads its raw UTF-8 text once on startup, then splits it on the
literal `.` character:

```kotlin
text.split('.')
    .map { it.trim() }
    .filter { it.isNotEmpty() }
```

Each non-empty piece becomes a message, without the separating period. Only
surrounding whitespace is removed; headings and internal line breaks remain.
`!` and `?` do not split messages, and periods in abbreviations do. This simple
rule is sufficient for the demo.

The producer sends one sentence at a time with a fresh UUID, using sender IDs
`alice`, `bob`, and `stepan` in that order, then repeats the sender rotation.
The Kafka key equals the message's `sender_id`. After each acknowledged send,
it waits one second by default. At the end of the sentence list it loops back
to the beginning, continuing the sender rotation independently. Restarting the
producer downloads the source again and starts from Alice and the first sentence.

The source must be reachable from the producer container. HTTP failures or an
empty sentence list stop the producer with an error. To use another UTF-8
plain-text URL or change the interval, set these Compose environment variables:

```bash
SENTENCE_SOURCE_URL=https://gist.githubusercontent.com/romaklimenko/c95f3a864828f7f034b7a33d1676e420/raw/warandpeace.txt \
PRODUCER_INTERVAL_MS=2000 docker-compose up -d producer
```

Pause and resume message generation independently of processing and logging:

```bash
docker-compose stop producer
docker-compose start producer
```

### Consumer offsets

The logger subscribes to `module2.chat.deliveries` with its own consumer group,
`module2-chat-logger`. It uses `auto.offset.reset=earliest`: on its first run,
with no committed offsets, it reads all retained output records. That also
covers records produced before the consumer finishes starting.

It auto-commits offsets every second while polling. On later runs, it resumes
from committed offsets. There are no `seek` calls or manual partition
assignments. A crash can cause some records to be logged again; this logger
does not promise exactly-once console output. Its offsets are independent of
the Streams application's input offsets.

### What to inspect

```bash
docker-compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 --describe --topic module2.chat.messages
docker-compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:29092 --describe --group module2-chat
```

- **Application ID:** `module2-chat` identifies this Streams application and its
  consumer group. Instances with the same ID share the work and committed input
  offsets. Changing the ID starts an independent application.
- **Tasks:** the message input has three partitions, producing three tasks for
  this topology. One configured stream thread handles those tasks; three tasks
  do not mean three application processes or three threads. A separate global
  thread maintains the complete user registry for the instance.
- **Partition and order:** each sender's key selects an input partition. The
  unchanged key and matching output partition count select the same partition
  number for the delivery envelope. Different senders can share a partition.
  Round-robin order describes the producer's sends; the consumer can interleave
  partitions differently. Kafka preserves order within each partition.
- **Offsets:** each topic-partition has its own sequence. Input and output
  offsets need not match even when their partition numbers do. The consumer-group
  report shows the application's committed input positions, not output offsets.
- **Restart position:** `earliest` applies only when no valid committed offset
  exists. Otherwise the application resumes from its committed position. The
  one-second commit interval makes progress visible during the exercise.
- **Processing guarantee:** this step uses the default at-least-once behavior.
  A failure can cause an input to be processed again. Transactional processing
  and recovery verification are part of step 9.

The demo now illustrates transport and recipient selection. The full
Alice/Bob/Stepan scenario becomes runnable as blocking, censorship, and statistics
are implemented. The producer supplies message text; all business processing
runs in the Streams application.

### Test recipient selection

From the repository root, with JDK 21 and Maven available:

```bash
mvn -Dkotlin.compiler.daemon=false test
```

The JUnit tests use Kafka's `TopologyTestDriver` with the real topology and
Protobuf records. They cover exclusion of the sender, exactly one envelope per
message, joining users, repeated registrations, tombstones and re-registration,
empty recipient lists, preserved message fields, and invalid IDs or keys.
The driver applies global-table input synchronously; the live demo has the
asynchronous update timing described above.

Verified checkpoint (2026-11-03): all seven topology tests passed locally and
in the Docker image's Maven `verify` build. The live demo restored the three
seeded users and selected the other two users for each sender. After publishing
Stepan's tombstone, Bob's recipient list changed from `[alice, stepan]` to
`[alice]`. Re-registering Stepan restores the original membership.

Stop the module-2 services with:

```bash
docker-compose down
```

Broker data and the application's state directory remain in named volumes.

## Implementation plan

Work through one step at a time. Before each change, explain the decision and
the Kafka Streams concept involved. Afterward, inspect the code, run a focused
example or test, and compare the result with the checkpoint. Discuss the result
before moving to the next step, and update this README as implementation
progresses.

- [x] **1. Define the behavior with concrete examples.**

    Specify recipients, directional blocking, censorship, rule updates, and the
    meaning of each statistic. Establish that messages with no eligible
    recipients still count and that recipient counts describe delivery
    decisions, not client acknowledgements.

    **Checkpoint:** the Alice/Bob/Stepan scenario above provides expected message
    text, recipients, and statistics to use throughout implementation.

- [x] **2. Define events, topics, and keys.**

    Introduce message, user, block/unblock, stop-word, and delivery-envelope
    models using the existing Protobuf tooling. Key messages by `senderId`,
    blocking commands by the blocked sender's ID, and user and stop-word updates
    by their respective identities. Define matching partitioning where records
    and state must be colocated.

    **Checkpoint:** trace "Alice blocks Bob" and "Bob sends hello" through their
    topic keys and explain where the related state will live.

- [x] **3. Run the smallest Kafka Streams application.**

    Add module-2 startup configuration, topic creation, and a topology that reads
    a message and wraps it in a delivery envelope with an empty recipient list.
    Preserve the message fields and sender key. Reuse the existing Maven/JVM
    setup. Keep topology construction separate from application startup so
    processing can be tested without a broker. Run the sentence producer and
    output logger as separate Compose services.

    **Checkpoint:** send one known message and inspect its key, value,
    partition, and output. Explain `application.id`, tasks, and offsets using
    this run.

- [x] **4. Add users and recipient selection.**

    Maintain the shared user registry in replicated local state. Calculate
    recipients for each message, exclude the sender, and populate the delivery
    envelope's recipient IDs.

    **Checkpoint:** with Alice, Bob, and Stepan registered, Bob's message targets
    Alice and Stepan. Explain how the envelope avoids a separate message record
    for every recipient.

- [ ] **5. Implement blocking.**

    Build the inverse index `blocked sender -> users who blocked that sender`
    and apply it during recipient selection. Add focused topology tests for
    block, unblock, repeated commands, and the direction of blocking. Explain
    when an update takes effect and why matching keys across separate topics
    do not establish a single ordering.

    **Checkpoint:** after Alice's block of Bob is applied, Bob's next message
    targets Stepan. Stepan's messages still target Alice and Bob. Unblocking
    restores Alice's eligibility for Bob's subsequent messages.

- [ ] **6. Add global censorship.**

    Maintain the shared stop-word state. Before recipient selection or
    statistics, replace case-insensitive, whole-word matches with exactly `***`
    while preserving surrounding text and punctuation. Censored messages remain
    eligible for delivery and count normally.

    **Checkpoint:** demonstrate adding and removing a stop word, mixed case,
    punctuation, repeated matches, a word inside a longer word, and a message
    whose entire text becomes `***`.

- [ ] **7. Add per-sender statistics.**

    Maintain each sender's message count and historical set of distinct
    recipients. Publish statistics as keyed updates. Count the original message
    once regardless of the number of recipient batches. Explain why the stores
    fit naturally beside sender-keyed processing.

    **Checkpoint:** Bob sends twice to Stepan: two messages and one unique
    recipient. After Alice unblocks Bob, another message brings Bob's totals to
    three messages and two unique recipients.

- [ ] **8. Add global statistics.**

    Count chat messages once globally. For unique recipients, emit newly
    encountered sender-recipient relationships and deduplicate by recipient in
    a separate aggregation. Inspect the required repartitioning and its cost:
    first-seen events add some fan-out, but repeated messages to the same
    recipients do not repeat it.

    **Checkpoint:** Bob and Stepan both target Alice, but Alice contributes only
    once to the global unique-recipient count. Verify the complete scenario's
    totals without summing per-sender unique counts.

- [ ] **9. Verify recovery and processing guarantees.**

    Restart the application and verify recovery of blocking state, recipient
    history, and counts. Configure transactional processing for Kafka outputs
    and state updates. Explain the guarantee's boundary: application-level
    duplicate submissions require a separate policy.

    **Checkpoint:** process a known sequence, restart, continue processing, and
    compare the recovered state and final totals with the expected results.

- [ ] **10. Run the complete scenario across two instances.**

    Run multiple Streams instances, inspect task assignment, and demonstrate
    how sender-owned state moves during a rebalance. Add reproducible demo
    commands and verification instructions to this README. Record remaining
    scaling limits, including hot senders and large recipient sets.

    **Checkpoint:** with rule updates applied before the corresponding messages,
    the example scenario produces the expected recipients and eventual
    statistics with either one instance or two.
