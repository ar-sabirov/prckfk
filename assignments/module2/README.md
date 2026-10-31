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

- [ ] **2. Define events, topics, and keys.**

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
