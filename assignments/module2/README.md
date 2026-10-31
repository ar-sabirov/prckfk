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

Register Alice, Bob, and Carol. Configure `bad` as a stop word and have Alice
block Bob. Apply these updates before processing the following messages.

| Step | Action | Delivered text | Eligible recipients | Global messages | Global unique recipients |
| --- | --- | --- | --- | --- | --- |
| 1 | Bob sends `This is BAD!` | `This is ***!` | Carol | 1 | 1 |
| 2 | Bob sends `Hello again` | `Hello again` | Carol | 2 | 1 |
| 3 | Carol sends `Hello` | `Hello` | Alice, Bob | 3 | 3 |
| 4 | Alice unblocks Bob; apply the update | — | — | 3 | 3 |
| 5 | Bob sends `bad badge` | `*** badge` | Alice, Carol | 4 | 3 |

Final per-user statistics:

| Sender | Messages | Unique recipients | Recipients reached |
| --- | --- | --- | --- |
| Alice | 0 | 0 | None |
| Bob | 3 | 2 | Alice, Carol |
| Carol | 1 | 2 | Alice, Bob |

Bob and Carol each reached two distinct recipients, but the global unique
recipient count is three because Alice appears in both sets.

## Events, topics, and keys

The wire models are defined in [chat.proto](../../src/main/proto/chat.proto).
Maven generates their Java classes in the `module2.chat.proto` package for use
from the JVM application. Topic names and partition counts are defined in
[ChatTopics.kt](../../src/main/kotlin/module2/chat/ChatTopics.kt).

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

These are the contracts for topic creation in step 3; no topics are created by
the models or constants. Statistics contracts will be added with steps 7 and 8.
Three sender partitions are a small demonstration choice that allows work to be
split across multiple tasks. This number is independent of the number of users;
many senders can share one partition.

Messages and block commands are event logs. In particular, do not compact block
commands by sender ID: Alice's block of Bob and Carol's block of Bob are separate
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
The users and stop-word tables will be replicated to every Streams instance;
their partition counts do not need to match the sender-partitioned inputs.

After the block and the stop word `bad` are applied, with Alice, Bob, and Carol
registered, the eventual delivery record is:

```text
topic: module2.chat.deliveries
key:   bob
value:
  message { message_id: "m-1" sender_id: "bob" text: "This is ***!" }
  recipient_ids: "carol"
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

    **Checkpoint:** the Alice/Bob/Carol scenario above provides expected message
    text, recipients, and statistics to use throughout implementation.

- [x] **2. Define events, topics, and keys.**

    Introduce message, user, block/unblock, stop-word, and delivery-envelope
    models using the existing Protobuf tooling. Key messages by `senderId`,
    blocking commands by the blocked sender's ID, and user and stop-word updates
    by their respective identities. Define matching partitioning where records
    and state must be colocated.

    **Checkpoint:** trace "Alice blocks Bob" and "Bob sends hello" through their
    topic keys and explain where the related state will live.

- [ ] **3. Run the smallest Kafka Streams application.**

    Add module-2 startup configuration, topic creation, and a topology that reads
    a message and writes it unchanged. Reuse the existing Maven/JVM setup. Keep
    topology construction separate from application startup so processing can
    be tested without a broker.

    **Checkpoint:** send one known message and inspect its key, value,
    partition, and output. Explain `application.id`, tasks, and offsets using
    this run.

- [ ] **4. Add users and recipient selection.**

    Maintain the shared user registry in replicated local state. Calculate
    recipients for each message, exclude the sender, and emit an envelope
    containing the message and recipient IDs.

    **Checkpoint:** with Alice, Bob, and Carol registered, Bob's message targets
    Alice and Carol. Explain how the envelope avoids a separate message record
    for every recipient.

- [ ] **5. Implement blocking.**

    Build the inverse index `blocked sender -> users who blocked that sender`
    and apply it during recipient selection. Add focused topology tests for
    block, unblock, repeated commands, and the direction of blocking. Explain
    when an update takes effect and why matching keys across separate topics
    do not establish a single ordering.

    **Checkpoint:** after Alice's block of Bob is applied, Bob's next message
    targets Carol. Carol's messages still target Alice and Bob. Unblocking
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

    **Checkpoint:** Bob sends twice to Carol: two messages and one unique
    recipient. After Alice unblocks Bob, another message brings Bob's totals to
    three messages and two unique recipients.

- [ ] **8. Add global statistics.**

    Count chat messages once globally. For unique recipients, emit newly
    encountered sender-recipient relationships and deduplicate by recipient in
    a separate aggregation. Inspect the required repartitioning and its cost:
    first-seen events add some fan-out, but repeated messages to the same
    recipients do not repeat it.

    **Checkpoint:** Bob and Carol both target Alice, but Alice contributes only
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
