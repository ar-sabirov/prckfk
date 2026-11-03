package module2

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.streams.StreamsConfig
import java.util.Properties

object ChatConfig {
    fun clientProperties(): Properties = Properties().apply {
        setProperty(
            CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
            System.getenv("KAFKA_BOOTSTRAP_SERVERS") ?: "localhost:19092",
        )
    }

    fun producerProperties(): Properties = clientProperties().apply {
        setProperty(ProducerConfig.ACKS_CONFIG, "all")
        setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
        setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "30000")
        setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "30000")
        setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000")
    }

    fun streamsProperties(): Properties = clientProperties().apply {
        setProperty(StreamsConfig.APPLICATION_ID_CONFIG, System.getenv("KAFKA_APPLICATION_ID") ?: "module2-chat")
        setProperty(StreamsConfig.STATE_DIR_CONFIG, System.getenv("KAFKA_STATE_DIR") ?: "target/module2-state")
        setProperty(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "1")
        setProperty(StreamsConfig.REPLICATION_FACTOR_CONFIG, "1")
        setProperty(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "1000")
        // This only applies when the application has no committed input offset.
        setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    }
}
