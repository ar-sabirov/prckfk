package module2

import module2.proto.ChatMessage
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringSerializer
import java.time.Duration
import java.util.UUID
import java.util.concurrent.TimeUnit

fun main() {
    val sourceUrl = System.getenv("SENTENCE_SOURCE_URL") ?: DEFAULT_SENTENCE_SOURCE_URL
    val intervalMs = (System.getenv("PRODUCER_INTERVAL_MS") ?: "1000").toLong()
    require(intervalMs > 0) { "PRODUCER_INTERVAL_MS must be positive" }
    val sentences = loadSentences(sourceUrl)
    println("Loaded ${sentences.size} sentences from $sourceUrl")
    val senders = listOf("alice", "bob", "stepan")

    KafkaProducer(ChatConfig.producerProperties(), StringSerializer(), ByteArraySerializer()).use { producer ->
        Runtime.getRuntime().addShutdownHook(Thread { producer.close(Duration.ofSeconds(10)) })
        var senderIndex = 0
        var sentenceIndex = 0
        while (true) {
            val message = ChatMessage.newBuilder()
                .setMessageId(UUID.randomUUID().toString())
                .setSenderId(senders[senderIndex])
                .setText(sentences[sentenceIndex])
                .build()
            val metadata = producer.send(
                ProducerRecord(ChatTopics.MESSAGES, message.senderId, message.toByteArray()),
            ).get(30, TimeUnit.SECONDS)
            println(
                "Produced topic=${metadata.topic()} partition=${metadata.partition()} offset=${metadata.offset()} " +
                    "key=${message.senderId} messageId=${message.messageId} text=\"${message.text}\"",
            )
            // Keep the sender rotation independent of the number of sentences.
            senderIndex = (senderIndex + 1) % senders.size
            sentenceIndex = (sentenceIndex + 1) % sentences.size
            Thread.sleep(intervalMs)
        }
    }
}
