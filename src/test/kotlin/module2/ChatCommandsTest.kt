package module2

import module2.proto.BlockCommand
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
    fun `membership commands publish users and tombstones`() {
        producer().use { producer ->
            val commands = ChatCommands(producer) {}
            commands.execute(listOf("add", "alice", "stepan"))
            commands.execute(listOf("remove", "alice"))
            val records = producer.history()
            assertEquals(listOf("alice", "stepan", "alice"), records.map { it.key() })
            assertTrue(records.all { it.topic() == ChatTopics.USERS })
            assertEquals("alice", User.parseFrom(records[0].value()).userId)
            assertEquals("stepan", User.parseFrom(records[1].value()).userId)
            assertNull(records[2].value())
        }
    }

    @Test
    fun `blocking commands use the blocked sender as key`() {
        producer().use { producer ->
            val commands = ChatCommands(producer) {}
            commands.execute(listOf("block", "alice", "bob"))
            commands.execute(listOf("unblock", "alice", "bob"))
            val records = producer.history()
            assertEquals(listOf("bob", "bob"), records.map { it.key() })
            assertTrue(records.all { it.topic() == ChatTopics.BLOCKED_USERS })
            val blocks = records.map { BlockCommand.parseFrom(it.value()) }
            assertTrue(blocks.all { it.blockedSenderId == "bob" && it.recipientId == "alice" })
            assertEquals(listOf(BlockCommand.Action.BLOCK, BlockCommand.Action.UNBLOCK), blocks.map { it.action })
        }
    }

    @Test
    fun `stop word commands normalize both additions and tombstone keys`() {
        producer().use { producer ->
            val commands = ChatCommands(producer) {}
            commands.execute(listOf("add-stop-word", " BAD ", "ВОЙНА"))
            commands.execute(listOf("remove-stop-word", " BAD "))
            val records = producer.history()
            assertEquals(listOf("bad", "война", "bad"), records.map { it.key() })
            assertTrue(records.all { it.topic() == ChatTopics.STOP_WORDS })
            assertEquals("bad", StopWord.parseFrom(records[0].value()).word)
            assertEquals("война", StopWord.parseFrom(records[1].value()).word)
            assertNull(records[2].value())
        }
    }

    @Test
    fun `invalid arguments publish no records even with a valid first argument`() {
        producer().use { producer ->
            val commands = ChatCommands(producer) {}
            val invalidCommands = listOf(
                emptyList(), listOf("unknown"), listOf("add"), listOf("remove"),
                listOf("add", "alice", ""), listOf("remove", "alice", ""),
                listOf("block", "alice"), listOf("unblock", "alice", "bob", "stepan"),
                listOf("block", "", "bob"), listOf("unblock", "alice", ""),
                listOf("add-stop-word"), listOf("remove-stop-word"),
                listOf("add-stop-word", "bad", "two words"), listOf("remove-stop-word", "bad", ""),
            )
            for (args in invalidCommands) {
                assertThrows(IllegalArgumentException::class.java) { commands.execute(args) }
                assertTrue(producer.history().isEmpty(), "Invalid command must not partially publish: $args")
            }
        }
    }

}
