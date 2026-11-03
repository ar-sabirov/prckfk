package module2

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Named
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.state.Stores

const val USERS_STORE = "chat-users"
const val STOP_WORDS_STORE = "chat-stop-words"
const val BLOCKED_BY_STORE = "chat-blocked-by"

fun buildChatTopology(): Topology {
    val builder = StreamsBuilder()
    // Every Streams instance restores all user partitions into its own store.
    builder.globalTable(
        ChatTopics.USERS,
        Consumed.with(Serdes.String(), Serdes.ByteArray()).withName("chat-users-input"),
        Materialized.`as`(USERS_STORE),
    )
    builder.globalTable(
        ChatTopics.STOP_WORDS,
        Consumed.with(Serdes.String(), Serdes.ByteArray()).withName("chat-stop-words-input"),
        Materialized.`as`(STOP_WORDS_STORE),
    )

    builder.addStateStore(
        Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(BLOCKED_BY_STORE),
            Serdes.String(),
            Serdes.ByteArray(),
        ),
    )
    // Connecting both processors to this store puts both sources in the same
    // subtopology. Matching sender keys and partition counts then keep commands,
    // messages, and the sender's blocking state together in the same task.
    builder.stream(
        ChatTopics.BLOCKED_USERS,
        Consumed.with(Serdes.String(), Serdes.ByteArray()).withName("chat-block-commands-input"),
    ).processValues(
        ::BlockCommandProcessor,
        Named.`as`("apply-block-command"),
        BLOCKED_BY_STORE,
    )

    builder.stream(
        ChatTopics.MESSAGES,
        Consumed.with(Serdes.String(), Serdes.ByteArray()).withName("chat-input"),
    ).processValues(
        ::CensorshipProcessor,
        Named.`as`("censor-message"),
    ).processValues(
        ::RecipientSelectionProcessor,
        Named.`as`("select-recipients"),
        BLOCKED_BY_STORE,
    ).to(
        ChatTopics.FILTERED_MESSAGES,
        Produced.with(Serdes.String(), Serdes.ByteArray()).withName("chat-deliveries"),
    )
    return builder.build()
}
