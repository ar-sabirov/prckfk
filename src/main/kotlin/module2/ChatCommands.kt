package module2

import module2.proto.BlockCommand
import module2.proto.StopWord
import module2.proto.User
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringSerializer
import java.util.concurrent.TimeUnit

val CHAT_COMMAND_HELP = """
    Commands:
      add <user-id> ...
      remove <user-id> ...
      block <recipient-id> <sender-id>
      unblock <recipient-id> <sender-id>
      add-stop-word <word> ...
      remove-stop-word <word> ...
      help
    Quote arguments containing spaces in your shell.
    Kafka acknowledgement does not mean Streams has applied the update yet.
""".trimIndent()

fun main(args: Array<String>) {
    if (args.isEmpty() || args.toList() == listOf("help")) {
        println(CHAT_COMMAND_HELP)
        return
    }
    KafkaProducer(ChatConfig.producerProperties(), StringSerializer(), ByteArraySerializer()).use { producer ->
        ChatCommands(producer).execute(args.toList())
    }
}

class ChatCommands(
    private val producer: Producer<String, ByteArray>,
    private val output: (String) -> Unit = ::println,
) {
    fun execute(args: List<String>) {
        require(args.isNotEmpty()) { CHAT_COMMAND_HELP }
        val action = args.first()
        val values = args.drop(1)
        // Validate the whole command before publishing any of its records.
        val records: List<ProducerRecord<String, ByteArray>> = when (action) {
            "add", "remove" -> {
                require(values.isNotEmpty() && values.all { it.isNotEmpty() }) { "Usage: $action <user-id> ..." }
                values.map { userId ->
                    val value = if (action == "add") User.newBuilder().setUserId(userId).build().toByteArray() else null
                    ProducerRecord(ChatTopics.USERS, userId, value)
                }
            }
            "block", "unblock" -> {
                require(values.size == 2 && values.all { it.isNotEmpty() }) {
                    "Usage: $action <recipient-id> <sender-id>"
                }
                val command = BlockCommand.newBuilder()
                    .setRecipientId(values[0])
                    .setBlockedSenderId(values[1])
                    .setAction(if (action == "block") BlockCommand.Action.BLOCK else BlockCommand.Action.UNBLOCK)
                    .build()
                listOf(ProducerRecord(ChatTopics.BLOCK_COMMANDS, command.blockedSenderId, command.toByteArray()))
            }
            "add-stop-word", "remove-stop-word" -> {
                require(values.isNotEmpty()) { "Usage: $action <word> ..." }
                values.map(::normalizeStopWord).map { word ->
                    val value = if (action == "add-stop-word") StopWord.newBuilder().setWord(word).build().toByteArray() else null
                    ProducerRecord(ChatTopics.STOP_WORDS, word, value)
                }
            }
            else -> throw IllegalArgumentException("Unknown command: $action. Run with help for available commands.")
        }
        for (record in records) {
            val metadata = producer.send(record).get(30, TimeUnit.SECONDS)
            output(
                "Published $action key=${record.key()} topic=${metadata.topic()} " +
                    "partition=${metadata.partition()} offset=${metadata.offset()}",
            )
        }
    }
}
