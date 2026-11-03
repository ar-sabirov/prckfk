package module2

object ChatTopics {
    const val MESSAGES = "module2.chat.messages"
    const val USERS = "module2.chat.users"
    const val BLOCK_COMMANDS = "module2.chat.block-commands"
    const val STOP_WORDS = "module2.chat.stop-words"
    const val DELIVERIES = "module2.chat.deliveries"

    // Messages and block commands must share this count and the key partitioner.
    // Delivery output keeps the sender key and uses this count as well.
    const val SENDER_PARTITIONS = 3

    // These small reference tables will be replicated to every Streams instance.
    const val REFERENCE_PARTITIONS = 1
}
