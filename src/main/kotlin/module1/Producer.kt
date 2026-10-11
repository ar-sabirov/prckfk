package module1

import module1.proto.OrderCreated
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties
import java.util.UUID
import java.util.concurrent.ExecutionException

fun producer(): KafkaProducer<String, ByteArray> {
    val properties = Properties().apply {
        // Начальные адреса брокеров: kafka:29092 в Compose, localhost:9092 при запуске на хосте.
        setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, System.getenv("KAFKA_BOOTSTRAP_SERVERS"))
        // Лидер ждёт подтверждения всех текущих ISR, не обязательно всех назначенных реплик.
        setProperty(ProducerConfig.ACKS_CONFIG, "all")
        // Строковый orderId кодируется в ключ; его хеш определяет партицию.
        setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        // Значение уже сериализовано через Protobuf; Kafka передаёт готовый массив байтов.
        setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer::class.java.name)

        // Защита от дубликатов при внутренних retries Kafka, но не при повторных вызовах send().
        setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
        // Разрешаем много повторов временных ошибок; фактическое время ограничено delivery.timeout.ms.
        setProperty(ProducerConfig.RETRIES_CONFIG, Int.MAX_VALUE.toString())
        // Бюджет доставки, включая ожидание в батче, подтверждения и retries: 60 секунд.
        setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "60000")
    }
    return KafkaProducer(properties, StringSerializer(), ByteArraySerializer())
}

private fun OrderCreated.serialize(): ByteArray {
    try {
        require(this.orderId.isNotBlank()) {
            "order_id must not be blank"
        }

        return this.toByteArray()
    } catch (error: RuntimeException) {
        System.err.println(
            "Cannot encode orderId=${this.orderId}: ${error.message}",
        )
        error.printStackTrace(System.err)
        throw error
    }
}

private fun KafkaProducer<String, ByteArray>.sendOrThrow(order: OrderCreated): RecordMetadata {
    try {
        val record = ProducerRecord(ORDERS, order.orderId, order.serialize())
        return this.send(record).get()
    } catch (error: ExecutionException) {
        val cause = error.cause ?: error

        System.err.println("Delivery not confirmed; stopping producer for orderId=${order.orderId}: ${cause.message}")
        cause.printStackTrace(System.err)
        throw error
    } catch (error: TimeoutException) {
        System.err.println("Timed out enqueueing; stopping producer for orderId=${order.orderId}: ${error.message}")
        error.printStackTrace(System.err)
        throw error
    } catch (error: SerializationException) {
        System.err.println("Kafka serialization failed for orderId=${order.orderId}")
        error.printStackTrace(System.err)
        throw error
    }
}

fun main() {
    producer().use { producer ->
        while (true) {
            val orderId = UUID.randomUUID().toString()
            val order = OrderCreated.newBuilder().setOrderId(orderId).build()

            val metadata = producer.sendOrThrow(order)
            println("Produced orderId=${order.orderId} partition=${metadata.partition()} offset=${metadata.offset()}")
            // Пауза после подтверждённой отправки; интервал включает также время send().get().
            Thread.sleep(1000)
        }
    }
}
