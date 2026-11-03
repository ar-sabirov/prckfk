package module2

import module2.proto.ChatMessage
import module2.proto.DeliveryEnvelope
import module2.proto.User
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.errors.StreamsException
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path
import java.util.Properties

class ChatTopologyTest {
    @TempDir
    lateinit var stateDir: Path

    private lateinit var driver: TopologyTestDriver
    private lateinit var users: TestInputTopic<String, ByteArray>
    private lateinit var messages: TestInputTopic<String, ByteArray>
    private lateinit var deliveries: TestOutputTopic<String, ByteArray>
    private var nextMessageId = 1

    @BeforeEach
    fun setUp() {
        val properties = Properties().apply {
            setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "module2-chat-test")
            setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "unused:9092")
            setProperty(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString())
        }
        driver = TopologyTestDriver(buildChatTopology(), properties)
        users = driver.createInputTopic(ChatTopics.USERS, StringSerializer(), ByteArraySerializer())
        messages = driver.createInputTopic(ChatTopics.MESSAGES, StringSerializer(), ByteArraySerializer())
        deliveries = driver.createOutputTopic(ChatTopics.DELIVERIES, StringDeserializer(), ByteArrayDeserializer())
    }

    @AfterEach
    fun tearDown() {
        if (::driver.isInitialized) driver.close()
    }

    @Test
    fun `each sender reaches the other registered users in one envelope`() {
        val members = listOf("alice", "bob", "stepan")
        members.forEach(::register)
        assertTrue(deliveries.isEmpty)

        for (sender in members) assertRecipients(sender, members.filter { it != sender })
    }

    @Test
    fun `joining users can receive without sending and repeated registration does not duplicate them`() {
        register("alice")
        assertRecipients("bob", listOf("alice"))

        register("stepan")
        register("stepan")
        assertTrue(deliveries.isEmpty)
        assertRecipients("bob", listOf("alice", "stepan"))
    }

    @Test
    fun `tombstones remove membership and registration can restore it`() {
        listOf("alice", "bob", "stepan").forEach(::register)
        users.pipeInput("alice", null)
        users.pipeInput("alice", null)
        users.pipeInput("absent-user", null)
        assertTrue(deliveries.isEmpty)
        assertRecipients("bob", listOf("stepan"))

        register("alice")
        assertRecipients("bob", listOf("alice", "stepan"))
    }

    @Test
    fun `empty registry and sender-only registry still emit one envelope`() {
        assertRecipients("bob", emptyList())
        register("bob")
        assertRecipients("bob", emptyList())
    }

    @Test
    fun `message sender must match the Kafka key`() {
        val message = ChatMessage.newBuilder().setMessageId("m-1").setSenderId("bob").setText("Hello").build()
        assertThrows(StreamsException::class.java) { messages.pipeInput("alice", message.toByteArray()) }
        assertTrue(deliveries.isEmpty)
    }

    @Test
    fun `message ID must not be empty`() {
        val message = ChatMessage.newBuilder().setSenderId("bob").setText("Hello").build()
        assertThrows(StreamsException::class.java) { messages.pipeInput("bob", message.toByteArray()) }
        assertTrue(deliveries.isEmpty)
    }

    @Test
    fun `user payload must match its registry key`() {
        users.pipeInput("alice", User.newBuilder().setUserId("stepan").build().toByteArray())
        val message = ChatMessage.newBuilder().setMessageId("m-1").setSenderId("bob").setText("Hello").build()
        assertThrows(StreamsException::class.java) { messages.pipeInput("bob", message.toByteArray()) }
        assertTrue(deliveries.isEmpty)
    }

    private fun register(userId: String) {
        users.pipeInput(userId, User.newBuilder().setUserId(userId).build().toByteArray())
    }

    private fun assertRecipients(sender: String, expected: List<String>) {
        val message = ChatMessage.newBuilder()
            .setMessageId("m-${nextMessageId++}")
            .setSenderId(sender)
            .setText("This is BAD! Привет.")
            .build()
        messages.pipeInput(sender, message.toByteArray())

        val record = deliveries.readKeyValue()
        val delivery = DeliveryEnvelope.parseFrom(record.value)
        assertEquals(sender, record.key)
        assertEquals(message, delivery.message)
        // Recipient order is not part of the contract; sorting keeps duplicate
        // IDs visible to the assertion, unlike converting the result to a set.
        assertEquals(expected.sorted(), delivery.recipientIdsList.sorted())
        assertTrue(deliveries.isEmpty, "Each message must produce exactly one envelope")
    }
}
