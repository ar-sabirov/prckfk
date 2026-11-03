package module2

import module2.proto.ChatMessage
import module2.proto.BlockedRecipients
import module2.proto.DeliveryEnvelope
import module2.proto.User
import org.apache.kafka.streams.processor.api.FixedKeyProcessor
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext
import org.apache.kafka.streams.processor.api.FixedKeyRecord
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.TimestampedKeyValueStore

class RecipientSelectionProcessor : FixedKeyProcessor<String, ByteArray, ByteArray> {
    private lateinit var context: FixedKeyProcessorContext<String, ByteArray>
    private lateinit var users: TimestampedKeyValueStore<String, ByteArray>
    private lateinit var blockedBy: KeyValueStore<String, ByteArray>

    override fun init(context: FixedKeyProcessorContext<String, ByteArray>) {
        this.context = context
        // Global stores are available to every processor without a connection
        // in processValues(). GlobalKTable stores a timestamp beside each value.
        users = context.getStateStore(USERS_STORE)
        blockedBy = context.getStateStore(BLOCKED_BY_STORE)
    }

    override fun process(record: FixedKeyRecord<String, ByteArray>) {
        val key = requireNotNull(record.key()) { "Chat message key must not be null" }
        require(key.isNotEmpty()) { "Chat message key must not be empty" }
        val message = ChatMessage.parseFrom(requireNotNull(record.value()) { "Chat messages cannot be tombstones" })
        require(message.messageId.isNotEmpty()) { "Chat message ID must not be empty" }
        require(message.senderId == key) { "Chat message sender_id must match its Kafka key" }

        val blockedRecipients = blockedBy.get(key)?.let {
            BlockedRecipients.parseFrom(it).recipientIdsList.toSet()
        } ?: emptySet()
        val delivery = DeliveryEnvelope.newBuilder().setMessage(message)
        users.all().use { entries ->
            while (entries.hasNext()) {
                val entry = entries.next()
                val user = User.parseFrom(entry.value.value())
                require(user.userId.isNotEmpty() && user.userId == entry.key) {
                    "User user_id must be non-empty and match its Kafka key"
                }
                if (user.userId != message.senderId && user.userId !in blockedRecipients) {
                    delivery.addRecipientIds(user.userId)
                }
            }
        }
        // One envelope, even with no eligible recipients. FixedKeyRecord
        // preserves the sender key and record metadata.
        context.forward(record.withValue(delivery.build().toByteArray()))
    }
}
