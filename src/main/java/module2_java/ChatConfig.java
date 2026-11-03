package module2_java;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public final class ChatConfig {
    private ChatConfig() {}

    public static Properties clientProperties() {
        Properties properties = new Properties();
        properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                environmentOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:19093"));
        return properties;
    }

    public static Properties producerProperties() {
        Properties properties = clientProperties();
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "30000");
        properties.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "30000");
        properties.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        return properties;
    }

    public static Properties streamsProperties() {
        Properties properties = clientProperties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,
                environmentOrDefault("KAFKA_APPLICATION_ID", "module2-java-chat"));
        properties.setProperty(StreamsConfig.STATE_DIR_CONFIG,
                environmentOrDefault("KAFKA_STATE_DIR", "target/module2-java-state"));
        properties.setProperty(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "1");
        properties.setProperty(StreamsConfig.REPLICATION_FACTOR_CONFIG, "1");
        properties.setProperty(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "1000");
        // This only applies when the application has no committed input offset.
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return properties;
    }

    static String environmentOrDefault(String name, String fallback) {
        String value = System.getenv(name);
        return value == null ? fallback : value;
    }
}
