package module2

import module2.proto.ChatMessage
import module2.proto.StopWord
import org.apache.kafka.streams.processor.api.FixedKeyProcessor
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext
import org.apache.kafka.streams.processor.api.FixedKeyRecord
import org.apache.kafka.streams.state.TimestampedKeyValueStore

class CensorshipProcessor : FixedKeyProcessor<String, ByteArray, ByteArray> {
    private lateinit var context: FixedKeyProcessorContext<String, ByteArray>
    private lateinit var stopWords: TimestampedKeyValueStore<String, ByteArray>

    override fun init(context: FixedKeyProcessorContext<String, ByteArray>) {
        this.context = context
        // Like the user registry, this global store needs no explicit connection.
        stopWords = context.getStateStore(STOP_WORDS_STORE)
    }

    override fun process(record: FixedKeyRecord<String, ByteArray>) {
        val message = ChatMessage.parseFrom(requireNotNull(record.value()) { "Chat messages cannot be tombstones" })
        val words = mutableSetOf<String>()
        stopWords.all().use { entries ->
            while (entries.hasNext()) {
                val entry = entries.next()
                val word = StopWord.parseFrom(entry.value.value()).word
                require(word == normalizeStopWord(word) && word == entry.key) {
                    "Stop-word key and word must match and already be normalized"
                }
                words.add(word)
            }
        }

        val censoredMessage = message.toBuilder().setText(censor(message.text, words)).build()
        // Preserve the key, ID, and metadata. Recipient selection validates the
        // sender and message ID and still emits an envelope when text is all ***.
        context.forward(record.withValue(censoredMessage.toByteArray()))
    }
}
