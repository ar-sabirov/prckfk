package module2

import module2.proto.BlockCommand
import module2.proto.ChatMessage
import module2.proto.StopWord
import module2.proto.User
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class ChatCommandsTest {
    private fun producer() = MockProducer(true, null, StringSerializer(), ByteArraySerializer())

    @Test
    fun `send publishes one message with the exact sender ID and text`() {
        producer().use { producer ->
            val output = mutableListOf<String>()
            val commands = ChatCommands(producer, output::add)
            commands.execute(listOf("send", "Степан", "m-1", "  Привет, BAD!\n"))
            val record = producer.history().single()
            assertEquals(ChatTopics.MESSAGES, record.topic())
            assertEquals("Степан", record.key())
            val message = ChatMessage.parseFrom(record.value())
            assertEquals("Степан", message.senderId)
            assertEquals("m-1", message.messageId)
            assertEquals("  Привет, BAD!\n", message.text)
            assertTrue(output.single().contains("messageId=m-1"))
        }
    }

    @Test
    fun `membership commands publish users and tombstones`() {
        producer().use { producer ->
            val commands = ChatCommands(producer) {}
            commands.execute(listOf("add", "Alice", "Степан"))
            commands.execute(listOf("remove", "Степан"))
            val records = producer.history()
            assertEquals(listOf("Alice", "Степан", "Степан"), records.map { it.key() })
            assertTrue(records.all { it.topic() == ChatTopics.USERS })
            assertEquals("Alice", User.parseFrom(records[0].value()).userId)
            assertEquals("Степан", User.parseFrom(records[1].value()).userId)
            assertNull(records[2].value())
        }
    }

    @Test
    fun `blocking commands use the blocked sender as key`() {
        producer().use { producer ->
            val commands = ChatCommands(producer) {}
            commands.execute(listOf("block", "Alice", "Степан"))
            commands.execute(listOf("unblock", "Alice", "Степан"))
            val records = producer.history()
            assertEquals(listOf("Степан", "Степан"), records.map { it.key() })
            assertTrue(records.all { it.topic() == ChatTopics.BLOCKED_USERS })
            val blocks = records.map { BlockCommand.parseFrom(it.value()) }
            assertTrue(blocks.all { it.blockedSenderId == "Степан" && it.recipientId == "Alice" })
            assertEquals(listOf(BlockCommand.Action.BLOCK, BlockCommand.Action.UNBLOCK), blocks.map { it.action })
        }
    }

    @Test
    fun `stop word commands normalize both additions and tombstone keys`() {
        producer().use { producer ->
            val commands = ChatCommands(producer) {}
            commands.execute(listOf("add-stop-word", " BAD ", "ПРИВЕТ"))
            commands.execute(listOf("remove-stop-word", " BAD "))
            val records = producer.history()
            assertEquals(listOf("bad", "привет", "bad"), records.map { it.key() })
            assertTrue(records.all { it.topic() == ChatTopics.STOP_WORDS })
            assertEquals("bad", StopWord.parseFrom(records[0].value()).word)
            assertEquals("привет", StopWord.parseFrom(records[1].value()).word)
            assertNull(records[2].value())
        }
    }

    @Test
    fun `invalid arguments publish no records even with a valid first argument`() {
        producer().use { producer ->
            val commands = ChatCommands(producer) {}
            val invalidCommands = listOf(
                emptyList(), listOf("unknown"), listOf("add"), listOf("remove"),
                listOf("add", "Alice", ""), listOf("remove", "Alice", ""),
                listOf("block", "Alice"), listOf("unblock", "Alice", "Bob", "Степан"),
                listOf("block", "", "Bob"), listOf("unblock", "Alice", ""),
                listOf("add-stop-word"), listOf("remove-stop-word"),
                listOf("add-stop-word", "bad", "two words"), listOf("remove-stop-word", "bad", ""),
                listOf("send"), listOf("send", "Bob", "m-1"),
                listOf("send", "", "m-1", "hello"), listOf("send", "Bob", "", "hello"),
                listOf("send", "Bob", "m-1", "hello", "extra"),
            )
            for (args in invalidCommands) {
                assertThrows(IllegalArgumentException::class.java) { commands.execute(args) }
                assertTrue(producer.history().isEmpty(), "Invalid command must not partially publish: $args")
            }
        }
    }

}
