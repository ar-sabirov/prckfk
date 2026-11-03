package module2

import module2.proto.ChatMessage
import module2.proto.DeliveryEnvelope
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Produced

fun buildChatTopology(): Topology {
    val builder = StreamsBuilder()
    // Step 3 wraps the message; step 4 will populate the recipients.
    builder.stream(
        ChatTopics.MESSAGES,
        Consumed.with(Serdes.String(), Serdes.ByteArray()).withName("chat-input"),
    ).mapValues { value ->
        DeliveryEnvelope.newBuilder()
            .setMessage(ChatMessage.parseFrom(value))
            .build()
            .toByteArray()
    }.to(
        ChatTopics.DELIVERIES,
        Produced.with(Serdes.String(), Serdes.ByteArray()).withName("chat-deliveries"),
    )
    return builder.build()
}
