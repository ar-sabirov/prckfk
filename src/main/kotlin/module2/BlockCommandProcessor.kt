package module2

import module2.proto.BlockCommand
import module2.proto.BlockedRecipients
import org.apache.kafka.streams.processor.api.FixedKeyProcessor
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext
import org.apache.kafka.streams.processor.api.FixedKeyRecord
import org.apache.kafka.streams.state.KeyValueStore

class BlockCommandProcessor : FixedKeyProcessor<String, ByteArray, Void> {
    private lateinit var blockedBy: KeyValueStore<String, ByteArray>

    override fun init(context: FixedKeyProcessorContext<String, Void>) {
        blockedBy = context.getStateStore(BLOCKED_BY_STORE)
    }

    override fun process(record: FixedKeyRecord<String, ByteArray>) {
        val key = requireNotNull(record.key()) { "Block command key must not be null" }
        require(key.isNotEmpty()) { "Block command key must not be empty" }
        val command = BlockCommand.parseFrom(requireNotNull(record.value()) {
            "Block commands cannot be tombstones; use UNBLOCK"
        })
        require(command.blockedSenderId == key) { "Block command blocked_sender_id must match its Kafka key" }
        require(command.recipientId.isNotEmpty()) { "Block command recipient_id must not be empty" }
        require(command.action == BlockCommand.Action.BLOCK || command.action == BlockCommand.Action.UNBLOCK) {
            "Block command action must be BLOCK or UNBLOCK"
        }

        val recipients = blockedBy.get(key)?.let {
            BlockedRecipients.parseFrom(it).recipientIdsList.toMutableSet()
        } ?: mutableSetOf()
        val changed = when (command.action) {
            BlockCommand.Action.BLOCK -> recipients.add(command.recipientId)
            else -> recipients.remove(command.recipientId)
        }
        if (changed) {
            if (recipients.isEmpty()) {
                blockedBy.delete(key)
            } else {
                blockedBy.put(key, BlockedRecipients.newBuilder().addAllRecipientIds(recipients).build().toByteArray())
            }
        }
        // A broker acknowledgement alone cannot confirm that this task applied a command.
        println("Applied ${command.action} recipient=${command.recipientId} sender=$key")
        // Commands update local state only; they never produce delivery records.
    }
}
