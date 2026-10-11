package module1

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.util.Properties

private fun consumer(): KafkaConsumer<String, ByteArray> {
    val properties = Properties().apply {
        // Начальные адреса брокеров из окружения; внутри Compose используются порты 29092.
        setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, System.getenv("KAFKA_BOOTSTRAP_SERVERS"))
        // Отдельная от Single группа получает собственную копию потока и хранит свои offsets.
        setProperty(ConsumerConfig.GROUP_ID_CONFIG, "batch")
        // Без пригодного коммита читаем с самой ранней сохранённой записи, а не обязательно с offset 0.
        setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        // Верхняя граница одного poll: 100 записей; минимальное количество эта настройка не задаёт.
        setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100")
        // Прогресс подтверждается вручную один раз после обработки накопленной пачки.
        setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
        // Отсутствие heartbeat в течение 10 секунд позволяет координатору перераспределить партиции.
        setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000")
        // Ключ Kafka декодируется как строковый orderId.
        setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        // Protobuf разбирается из байтов в общей process(), а не внутри poll().
        setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer::class.java.name)
    }
    return KafkaConsumer(properties, StringDeserializer(), ByteArrayDeserializer())
}

fun main() {
    consumer().use { consumer ->
        // Минимум 10 гарантируется для обработки: записи могут накапливаться из нескольких poll().
        val batch = mutableListOf<ConsumerRecord<String, ByteArray>>()

        consumer.subscribe(
            listOf(ORDERS),
            object : ConsumerRebalanceListener {
                override fun onPartitionsRevoked(partitions: Collection<TopicPartition>) {
                    // Не обрабатываем буфер отозванных партиций: неподтверждённые записи перечитает владелец.
                    batch.removeAll { record ->
                        TopicPartition(record.topic(), record.partition()) in partitions
                    }
                }

                override fun onPartitionsAssigned(partitions: Collection<TopicPartition>) {
                    // Kafka восстановит позицию по коммиту группы или auto.offset.reset.
                }
            }
        )

        while (true) {
            val records = try {
                // Ожидание результата до секунды не задаёт минимальный размер результата poll().
                consumer.poll(Duration.ofSeconds(1))
            } catch (error: TimeoutException) {
                System.err.println("Polling timed out: ${error.message}")
                error.printStackTrace(System.err)
                continue
            }
            println("Records in this poll: ${records.count()}")
            batch.addAll(records)
            println("Accumulated batch size: ${batch.size}")

            if (batch.size >= 10) {
                println("Processing batch of ${batch.size} messages")

                batch.forEach { record ->
                    process(record)
                }

                // Подтверждаем всю обработанную пачку, включая записи, залогированные и пропущенные process().
                consumer.commitSync()
                batch.clear()
            }

        }
    }
}
