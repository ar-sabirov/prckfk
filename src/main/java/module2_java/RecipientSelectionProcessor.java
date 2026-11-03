package module2_java;

import com.google.protobuf.InvalidProtocolBufferException;
import module2.proto.BlockedRecipients;
import module2.proto.ChatMessage;
import module2.proto.DeliveryEnvelope;
import module2.proto.User;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;

import java.util.HashSet;
import java.util.Set;

public final class RecipientSelectionProcessor implements FixedKeyProcessor<String, byte[], byte[]> {
    private FixedKeyProcessorContext<String, byte[]> context;
    private TimestampedKeyValueStore<String, byte[]> users;
    private KeyValueStore<String, byte[]> blockedBy;

    @Override
    public void init(FixedKeyProcessorContext<String, byte[]> context) {
        this.context = context;
        // GlobalKTable stores a timestamp beside each value and is available to every processor.
        users = context.getStateStore(ChatTopology.USERS_STORE);
        blockedBy = context.getStateStore(ChatTopology.BLOCKED_BY_STORE);
    }

    @Override
    public void process(FixedKeyRecord<String, byte[]> record) {
        String key = record.key();
        if (key == null) {
            throw new IllegalArgumentException("Chat message key must not be null");
        }
        if (key.isEmpty()) {
            throw new IllegalArgumentException("Chat message key must not be empty");
        }
        if (record.value() == null) {
            throw new IllegalArgumentException("Chat messages cannot be tombstones");
        }
        try {
            ChatMessage message = ChatMessage.parseFrom(record.value());
            if (message.getMessageId().isEmpty()) {
                throw new IllegalArgumentException("Chat message ID must not be empty");
            }
            if (!message.getSenderId().equals(key)) {
                throw new IllegalArgumentException("Chat message sender_id must match its Kafka key");
            }
            byte[] stored = blockedBy.get(key);
            Set<String> blockedRecipients = stored == null ? Set.of()
                    : new HashSet<>(BlockedRecipients.parseFrom(stored).getRecipientIdsList());
            DeliveryEnvelope.Builder delivery = DeliveryEnvelope.newBuilder().setMessage(message);
            try (var entries = users.all()) {
                while (entries.hasNext()) {
                    var entry = entries.next();
                    User user = User.parseFrom(entry.value.value());
                    if (user.getUserId().isEmpty() || !user.getUserId().equals(entry.key)) {
                        throw new IllegalArgumentException("User user_id must be non-empty and match its Kafka key");
                    }
                    if (!user.getUserId().equals(message.getSenderId()) && !blockedRecipients.contains(user.getUserId())) {
                        delivery.addRecipientIds(user.getUserId());
                    }
                }
            }
            // Emit one envelope even with no recipients; retain the sender key and metadata.
            context.forward(record.withValue(delivery.build().toByteArray()));
        } catch (InvalidProtocolBufferException error) {
            throw new IllegalArgumentException("Invalid chat message, user, or blocking state Protobuf", error);
        }
    }
}
