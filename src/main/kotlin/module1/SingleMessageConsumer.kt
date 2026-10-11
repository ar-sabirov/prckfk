package module1

import com.google.protobuf.InvalidProtocolBufferException
import module1.proto.OrderCreated
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.util.Properties

private fun consumer(): KafkaConsumer<String, ByteArray> {
    val properties = Properties().apply {
        // Начальные адреса брокеров из окружения; внутри Compose используются порты 29092.
        setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, System.getenv("KAFKA_BOOTSTRAP_SERVERS"))
        // Реплики Single делят партиции этой группы; группа Batch читает топик независимо.
        setProperty(ConsumerConfig.GROUP_ID_CONFIG, "single")
        // Без пригодного коммита читаем с самой ранней сохранённой записи, а не обязательно с offset 0.
        setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        // Один poll возвращает не больше одной записи; пустой результат тоже допустим.
        setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1")
        // Kafka автоматически сохраняет прогресс; обработка выполняется до следующего poll.
        setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
        // Интервал автокоммита: 5 секунд, а не отдельный коммит сразу после каждой записи.
        setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000")
        // Без heartbeat в течение 10 секунд координатор может исключить consumer из группы.
        setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000")
        // Ключ Kafka декодируется как строковый orderId.
        setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        // Получаем сырые байты; Protobuf разбирается в process(), где можно обработать ошибку записи.
        setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer::class.java.name)
    }
    return KafkaConsumer(properties, StringDeserializer(), ByteArrayDeserializer())
}

fun process(record: ConsumerRecord<String, ByteArray>) {
    val bytes = record.value()
    if (bytes == null) {
        System.err.println("Null value: topic=${record.topic()} partition=${record.partition()} offset=${record.offset()}")
        return
    }

    val order = try {
        OrderCreated.parseFrom(bytes)
    } catch (error: InvalidProtocolBufferException) {
        val location =
            "topic=${record.topic()} partition=${record.partition()} offset=${record.offset()}"
        System.err.println("Protobuf decoding failed: $location")
        error.printStackTrace(System.err)
        return
    }

    try {
        require(order.orderId.isNotBlank()) { "order_id must not be blank" }
        println("Consumed orderId=${order.orderId} partition=${record.partition()} offset=${record.offset()}")
    } catch (e: Exception) {
        System.err.println("Error processing topic=${record.topic()} partition=${record.partition()} offset=${record.offset()}: ${e.message}")
    }
}

fun main() {
    consumer().use { consumer ->
        consumer.subscribe(listOf(ORDERS))

        while (true) {
            val records = try {
                // Ожидание одного poll до секунды; это не задержка между сообщениями.
                consumer.poll(Duration.ofSeconds(1))
            } catch (error: TimeoutException) {
                System.err.println("Polling timed out: ${error.message}")
                error.printStackTrace(System.err)
                continue
            }
            records.forEach { record ->
                process(record)
            }
        }
    }
}
