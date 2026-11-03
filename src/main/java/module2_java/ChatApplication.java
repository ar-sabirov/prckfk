package module2_java;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public final class ChatApplication {
    private ChatApplication() {}

    public static void main(String[] args) throws InterruptedException {
        var topology = ChatTopology.buildChatTopology();
        System.out.println(topology.describe());
        var streams = new KafkaStreams(topology, ChatConfig.streamsProperties());
        var stopped = new CountDownLatch(1);
        var failed = new AtomicBoolean(false);

        streams.setStateListener((newState, oldState) -> {
            System.out.printf("Chat Streams state: %s -> %s%n", oldState, newState);
            if (newState == KafkaStreams.State.ERROR) {
                failed.set(true);
                stopped.countDown();
            }
        });
        streams.setUncaughtExceptionHandler(error -> {
            error.printStackTrace();
            failed.set(true);
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        });
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close(Duration.ofSeconds(10));
            stopped.countDown();
        }));

        try {
            streams.start();
            stopped.await();
        } finally {
            streams.close(Duration.ofSeconds(10));
        }
        if (failed.get()) {
            System.exit(1);
        }
    }
}
