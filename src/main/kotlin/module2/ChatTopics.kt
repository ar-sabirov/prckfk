package module2

object ChatTopics {
    const val MESSAGES = "messages"
    const val USERS = "users"
    // An event log of block/unblock commands, not a compacted block snapshot.
    const val BLOCKED_USERS = "blocked_users"
    const val STOP_WORDS = "stop_words"
    const val FILTERED_MESSAGES = "filtered_messages"

    // Messages and block commands must share this count and the key partitioner.
    // Delivery output keeps the sender key and uses this count as well.
    const val SENDER_PARTITIONS = 3

    // These small reference tables will be replicated to every Streams instance.
    const val REFERENCE_PARTITIONS = 1
}
