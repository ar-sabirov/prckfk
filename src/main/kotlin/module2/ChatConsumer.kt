package module2

import module2.proto.DeliveryEnvelope
import org.apache.kafka.clients.consumer.CloseOptions
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

private val consumerProperties = ChatConfig.clientProperties().apply {
    setProperty(ConsumerConfig.GROUP_ID_CONFIG, System.getenv("KAFKA_CONSUMER_GROUP_ID") ?: "module2-chat-logger")
    setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
}

fun main() {
    val consumer = KafkaConsumer(consumerProperties, StringDeserializer(), ByteArrayDeserializer())
    val closed = CountDownLatch(1)
    // KafkaConsumer is not thread-safe. Wake up poll here; its owning thread
    // performs close and the final offset commit in the finally block below.
    Runtime.getRuntime().addShutdownHook(Thread {
        consumer.wakeup()
        closed.await(10, TimeUnit.SECONDS)
    })

    try {
        consumer.subscribe(listOf(ChatTopics.DELIVERIES))
        while (true) {
            for (record in consumer.poll(Duration.ofMillis(500))) {
                val delivery = DeliveryEnvelope.parseFrom(record.value())
                val message = delivery.message
                println(
                    "Consumed topic=${record.topic()} partition=${record.partition()} offset=${record.offset()} " +
                        "key=${record.key()} messageId=${message.messageId} " +
                        "recipients=${delivery.recipientIdsList} text=\"${message.text}\"",
                )
            }
        }
    } catch (_: WakeupException) {
        // Normal shutdown requested by the hook.
    } finally {
        try {
            consumer.close(CloseOptions.timeout(Duration.ofSeconds(5)))
        } finally {
            closed.countDown()
        }
    }
}
