package module2_java;

import com.google.protobuf.InvalidProtocolBufferException;
import module2.proto.DeliveryEnvelope;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public final class ChatConsumer {
    private ChatConsumer() {}

    private static Properties consumerProperties() {
        Properties properties = ChatConfig.clientProperties();
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,
                ChatConfig.environmentOrDefault("KAFKA_CONSUMER_GROUP_ID", "module2-java-chat-logger"));
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        return properties;
    }

    public static void main(String[] args) throws InvalidProtocolBufferException {
        var consumer = new KafkaConsumer<>(consumerProperties(), new StringDeserializer(), new ByteArrayDeserializer());
        var closed = new CountDownLatch(1);
        // Wake up poll; the owning thread performs close and the final offset commit.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumer.wakeup();
            try {
                closed.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
            }
        }));

        try {
            consumer.subscribe(List.of(ChatTopics.FILTERED_MESSAGES));
            while (true) {
                for (var record : consumer.poll(Duration.ofMillis(500))) {
                    var delivery = DeliveryEnvelope.parseFrom(record.value());
                    var message = delivery.getMessage();
                    System.out.printf("Consumed topic=%s partition=%d offset=%d key=%s messageId=%s recipients=%s text=\"%s\"%n",
                            record.topic(), record.partition(), record.offset(), record.key(), message.getMessageId(),
                            delivery.getRecipientIdsList(), message.getText());
                }
            }
        } catch (WakeupException ignored) {
            // Normal shutdown requested by the hook.
        } finally {
            try {
                consumer.close(CloseOptions.timeout(Duration.ofSeconds(5)));
            } finally {
                closed.countDown();
            }
        }
    }
}
