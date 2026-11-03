package module2_java;

import com.google.protobuf.InvalidProtocolBufferException;
import module2.proto.ChatMessage;
import module2.proto.StopWord;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;

import java.util.HashSet;
import java.util.Set;

public final class CensorshipProcessor implements FixedKeyProcessor<String, byte[], byte[]> {
    private FixedKeyProcessorContext<String, byte[]> context;
    private TimestampedKeyValueStore<String, byte[]> stopWords;

    @Override
    public void init(FixedKeyProcessorContext<String, byte[]> context) {
        this.context = context;
        // Global stores are available without an explicit processor connection.
        stopWords = context.getStateStore(ChatTopology.STOP_WORDS_STORE);
    }

    @Override
    public void process(FixedKeyRecord<String, byte[]> record) {
        if (record.value() == null) {
            throw new IllegalArgumentException("Chat messages cannot be tombstones");
        }
        try {
            ChatMessage message = ChatMessage.parseFrom(record.value());
            Set<String> words = new HashSet<>();
            try (var entries = stopWords.all()) {
                while (entries.hasNext()) {
                    var entry = entries.next();
                    String word = StopWord.parseFrom(entry.value.value()).getWord();
                    if (!word.equals(Censorship.normalizeStopWord(word)) || !word.equals(entry.key)) {
                        throw new IllegalArgumentException("Stop-word key and word must match and already be normalized");
                    }
                    words.add(word);
                }
            }
            ChatMessage censoredMessage = message.toBuilder()
                    .setText(Censorship.censor(message.getText(), words)).build();
            // Keep the key, ID, and metadata, even when every word is censored.
            context.forward(record.withValue(censoredMessage.toByteArray()));
        } catch (InvalidProtocolBufferException error) {
            throw new IllegalArgumentException("Invalid chat message or stop-word Protobuf", error);
        }
    }
}
