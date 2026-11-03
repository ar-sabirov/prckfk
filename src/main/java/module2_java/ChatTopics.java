package module2_java;

public final class ChatTopics {
    public static final String MESSAGES = "messages";
    public static final String USERS = "users";
    // An event log of block/unblock commands, not a compacted block snapshot.
    public static final String BLOCKED_USERS = "blocked_users";
    public static final String STOP_WORDS = "stop_words";
    public static final String FILTERED_MESSAGES = "filtered_messages";

    // Messages, block commands, and deliveries share the sender key and partition count.
    public static final int SENDER_PARTITIONS = 3;
    // These small reference tables are replicated to every Streams instance.
    public static final int REFERENCE_PARTITIONS = 1;

    private ChatTopics() {}
}
