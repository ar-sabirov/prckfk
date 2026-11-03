package module2

import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.system.exitProcess

fun main() {
    val topology = buildChatTopology()
    println(topology.describe())
    val streams = KafkaStreams(topology, ChatConfig.streamsProperties())
    val stopped = CountDownLatch(1)
    val failed = AtomicBoolean(false)

    streams.setStateListener { newState, oldState ->
        println("Chat Streams state: $oldState -> $newState")
        if (newState == KafkaStreams.State.ERROR) {
            failed.set(true)
            stopped.countDown()
        }
    }
    streams.setUncaughtExceptionHandler { error ->
        error.printStackTrace()
        failed.set(true)
        StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT
    }
    Runtime.getRuntime().addShutdownHook(Thread {
        streams.close(Duration.ofSeconds(10))
        stopped.countDown()
    })

    try {
        streams.start()
        stopped.await()
    } finally {
        streams.close(Duration.ofSeconds(10))
    }
    if (failed.get()) exitProcess(1)
}
