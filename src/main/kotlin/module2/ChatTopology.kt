package module2

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Named
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.state.KeyValueStore

const val USERS_STORE = "chat-users"

fun buildChatTopology(): Topology {
    val builder = StreamsBuilder()
    // Every Streams instance restores all user partitions into its own store.
    builder.globalTable(
        ChatTopics.USERS,
        Consumed.with(Serdes.String(), Serdes.ByteArray()).withName("chat-users-input"),
        Materialized.`as`(USERS_STORE),
    )

    builder.stream(
        ChatTopics.MESSAGES,
        Consumed.with(Serdes.String(), Serdes.ByteArray()).withName("chat-input"),
    ).processValues(
        ::RecipientSelectionProcessor,
        Named.`as`("select-recipients"),
    ).to(
        ChatTopics.DELIVERIES,
        Produced.with(Serdes.String(), Serdes.ByteArray()).withName("chat-deliveries"),
    )
    return builder.build()
}
