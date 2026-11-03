package module2

import module2.proto.BlockCommand
import module2.proto.ChatMessage
import module2.proto.DeliveryEnvelope
import module2.proto.StopWord
import module2.proto.User
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.TopologyDescription
import org.apache.kafka.streams.errors.StreamsException
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.nio.file.Path
import java.util.Properties

class ChatTopologyTest {
    @TempDir
    lateinit var stateDir: Path

    private lateinit var driver: TopologyTestDriver
    private lateinit var users: TestInputTopic<String, ByteArray>
    private lateinit var messages: TestInputTopic<String, ByteArray>
    private lateinit var blockCommands: TestInputTopic<String, ByteArray>
    private lateinit var stopWords: TestInputTopic<String, ByteArray>
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
        blockCommands = driver.createInputTopic(ChatTopics.BLOCKED_USERS, StringSerializer(), ByteArraySerializer())
        stopWords = driver.createInputTopic(ChatTopics.STOP_WORDS, StringSerializer(), ByteArraySerializer())
        deliveries = driver.createOutputTopic(ChatTopics.FILTERED_MESSAGES, StringDeserializer(), ByteArrayDeserializer())
    }

    @AfterEach
    fun tearDown() {
        if (::driver.isInitialized) driver.close()
    }

    @Test
    fun `each sender reaches the other registered users in one envelope`() {
        val members = listOf("Alice", "Bob", "Степан")
        members.forEach(::register)
        assertTrue(deliveries.isEmpty)

        for (sender in members) assertRecipients(sender, members.filter { it != sender })
    }

    @Test
    fun `joining users can receive without sending and repeated registration does not duplicate them`() {
        register("Alice")
        assertRecipients("Bob", listOf("Alice"))

        register("Степан")
        register("Степан")
        assertTrue(deliveries.isEmpty)
        assertRecipients("Bob", listOf("Alice", "Степан"))
    }

    @Test
    fun `tombstones remove membership and registration can restore it`() {
        listOf("Alice", "Bob", "Степан").forEach(::register)
        users.pipeInput("Alice", null)
        users.pipeInput("Alice", null)
        users.pipeInput("absent-user", null)
        assertTrue(deliveries.isEmpty)
        assertRecipients("Bob", listOf("Степан"))

        register("Alice")
        assertRecipients("Bob", listOf("Alice", "Степан"))
    }

    @Test
    fun `empty registry and sender-only registry still emit one envelope`() {
        assertRecipients("Bob", emptyList())
        register("Bob")
        assertRecipients("Bob", emptyList())
    }

    @Test
    fun `blocking is directional and unblocking affects subsequent messages`() {
        listOf("Alice", "Bob", "Степан").forEach(::register)
        assertRecipients("Bob", listOf("Alice", "Степан"))

        updateBlock("Alice", "Bob", BlockCommand.Action.BLOCK)
        assertRecipients("Bob", listOf("Степан"))
        assertRecipients("Alice", listOf("Bob", "Степан"))
        assertRecipients("Степан", listOf("Alice", "Bob"))

        updateBlock("Alice", "Bob", BlockCommand.Action.UNBLOCK)
        assertRecipients("Bob", listOf("Alice", "Степан"))
    }

    @Test
    fun `repeated commands have set semantics and unblocking preserves other blockers`() {
        listOf("Alice", "Bob", "Степан").forEach(::register)
        updateBlock("Alice", "Bob", BlockCommand.Action.UNBLOCK)
        assertRecipients("Bob", listOf("Alice", "Степан"))

        updateBlock("Alice", "Bob", BlockCommand.Action.BLOCK)
        updateBlock("Alice", "Bob", BlockCommand.Action.BLOCK)
        assertRecipients("Bob", listOf("Степан"))
        updateBlock("Степан", "Bob", BlockCommand.Action.BLOCK)
        updateBlock("Степан", "Bob", BlockCommand.Action.BLOCK)
        assertRecipients("Bob", emptyList())

        updateBlock("Alice", "Bob", BlockCommand.Action.UNBLOCK)
        updateBlock("Alice", "Bob", BlockCommand.Action.UNBLOCK)
        updateBlock("absent-user", "Bob", BlockCommand.Action.UNBLOCK)
        assertRecipients("Bob", listOf("Alice"))

        updateBlock("Степан", "Bob", BlockCommand.Action.UNBLOCK)
        updateBlock("Степан", "Bob", BlockCommand.Action.UNBLOCK)
        assertRecipients("Bob", listOf("Alice", "Степан"))
        updateBlock("Alice", "Bob", BlockCommand.Action.BLOCK)
        assertRecipients("Bob", listOf("Степан"))
    }

    @Test
    fun `each sender has an independent inverse blocking index`() {
        listOf("Alice", "Bob", "Степан").forEach(::register)
        updateBlock("Alice", "Bob", BlockCommand.Action.BLOCK)
        updateBlock("Alice", "Степан", BlockCommand.Action.BLOCK)
        updateBlock("Bob", "Степан", BlockCommand.Action.BLOCK)
        assertRecipients("Bob", listOf("Степан"))
        assertRecipients("Степан", emptyList())
        assertRecipients("Alice", listOf("Bob", "Степан"))

        updateBlock("Alice", "Bob", BlockCommand.Action.UNBLOCK)
        assertRecipients("Bob", listOf("Alice", "Степан"))
        assertRecipients("Степан", emptyList())
    }

    @Test
    fun `blocking is independent of membership changes`() {
        updateBlock("Alice", "Bob", BlockCommand.Action.BLOCK)
        listOf("Alice", "Bob", "Степан").forEach(::register)
        assertRecipients("Bob", listOf("Степан"))
        users.pipeInput("Alice", null)
        register("Alice")
        assertRecipients("Bob", listOf("Степан"))

        updateBlock("Alice", "Bob", BlockCommand.Action.UNBLOCK)
        assertRecipients("Bob", listOf("Alice", "Степан"))
    }

    @ParameterizedTest
    @MethodSource("censorshipExamples")
    fun `censorship replaces whole words and preserves surrounding text`(text: String, expectedText: String) {
        listOf("Alice", "Bob", "Степан").forEach(::register)
        listOf("bad", "привет", "café").forEach(::addStopWord)
        assertRecipients("Bob", listOf("Alice", "Степан"), text, expectedText)
    }

    @Test
    fun `adding and removing stop words changes only subsequent messages`() {
        register("Alice")
        assertRecipients("Bob", listOf("Alice"), "bad evil", "bad evil")
        addStopWord(" BAD ")
        addStopWord("bad")
        assertRecipients("Bob", listOf("Alice"), "bad evil", "*** evil")
        addStopWord("evil")
        assertRecipients("Bob", listOf("Alice"), "bad evil", "*** ***")

        stopWords.pipeInput("bad", null)
        stopWords.pipeInput("bad", null)
        stopWords.pipeInput("absent", null)
        assertTrue(deliveries.isEmpty, "Rule updates must not emit deliveries")
        assertRecipients("Bob", listOf("Alice"), "BAD evil", "BAD ***")
        stopWords.pipeInput("evil", null)
        assertTrue(deliveries.isEmpty)
        assertRecipients("Bob", listOf("Alice"), "BAD evil", "BAD evil")
        addStopWord("bad")
        assertRecipients("Bob", listOf("Alice"), "BAD evil", "*** evil")
    }

    @Test
    fun `every sender uses the same global censorship rules`() {
        val members = listOf("Alice", "Bob", "Степан")
        members.forEach(::register)
        addStopWord("bad")
        for (sender in members) {
            assertRecipients(sender, members.filter { it != sender }, "Bad badge", "*** badge")
        }
    }

    @Test
    fun `fully censored messages still emit one envelope with final eligible recipients`() {
        addStopWord("bad")
        assertRecipients("Bob", emptyList(), "BAD", "***")
        listOf("Alice", "Bob", "Степан").forEach(::register)
        updateBlock("Alice", "Bob", BlockCommand.Action.BLOCK)
        assertRecipients("Bob", listOf("Степан"), "BAD", "***")
        updateBlock("Степан", "Bob", BlockCommand.Action.BLOCK)
        assertRecipients("Bob", emptyList(), "bad", "***")
        updateBlock("Alice", "Bob", BlockCommand.Action.UNBLOCK)
        assertRecipients("Bob", listOf("Alice"), "bad", "***")
    }

    @ParameterizedTest(name = "reject invalid stop word: {0}")
    @MethodSource("invalidStopWords")
    fun `invalid stop words fail processing when the table is read`(description: String, key: String, value: ByteArray) {
        stopWords.pipeInput(key, value)
        val message = ChatMessage.newBuilder().setMessageId("m-1").setSenderId("Bob").setText("Hello").build()
        assertThrows(StreamsException::class.java, { messages.pipeInput("Bob", message.toByteArray()) }, description)
        assertTrue(deliveries.isEmpty)
    }

    @Test
    fun `messages and block commands share the store in one subtopology`() {
        val subtopology = buildChatTopology().describe().subtopologies().single()
        val inputTopics = subtopology.nodes().filterIsInstance<TopologyDescription.Source>()
            .flatMap { it.topicSet() }.toSet()
        assertEquals(setOf(ChatTopics.MESSAGES, ChatTopics.BLOCKED_USERS), inputTopics)
        val storeUsers = subtopology.nodes().filterIsInstance<TopologyDescription.Processor>()
            .filter { BLOCKED_BY_STORE in it.stores() }.map { it.name() }.toSet()
        assertEquals(setOf("apply-block-command", "select-recipients"), storeUsers)
    }

    @ParameterizedTest(name = "reject invalid block command: {0}")
    @MethodSource("invalidBlockCommands")
    fun `invalid block commands fail processing`(description: String, key: String?, value: ByteArray?) {
        assertThrows(StreamsException::class.java, { blockCommands.pipeInput(key, value) }, description)
        assertTrue(deliveries.isEmpty)
    }

    @Test
    fun `message sender must match the Kafka key`() {
        val message = ChatMessage.newBuilder().setMessageId("m-1").setSenderId("Bob").setText("Hello").build()
        assertThrows(StreamsException::class.java) { messages.pipeInput("Alice", message.toByteArray()) }
        assertTrue(deliveries.isEmpty)
    }

    @Test
    fun `message ID must not be empty`() {
        val message = ChatMessage.newBuilder().setSenderId("Bob").setText("Hello").build()
        assertThrows(StreamsException::class.java) { messages.pipeInput("Bob", message.toByteArray()) }
        assertTrue(deliveries.isEmpty)
    }

    @Test
    fun `user payload must match its registry key`() {
        users.pipeInput("Alice", User.newBuilder().setUserId("Степан").build().toByteArray())
        val message = ChatMessage.newBuilder().setMessageId("m-1").setSenderId("Bob").setText("Hello").build()
        assertThrows(StreamsException::class.java) { messages.pipeInput("Bob", message.toByteArray()) }
        assertTrue(deliveries.isEmpty)
    }

    private fun register(userId: String) {
        users.pipeInput(userId, User.newBuilder().setUserId(userId).build().toByteArray())
    }

    private fun addStopWord(input: String) {
        val word = normalizeStopWord(input)
        stopWords.pipeInput(word, StopWord.newBuilder().setWord(word).build().toByteArray())
        assertTrue(deliveries.isEmpty, "Rule updates must not emit deliveries")
    }

    private fun updateBlock(recipient: String, sender: String, action: BlockCommand.Action) {
        val command = BlockCommand.newBuilder()
            .setBlockedSenderId(sender).setRecipientId(recipient).setAction(action).build()
        blockCommands.pipeInput(sender, command.toByteArray())
        assertTrue(deliveries.isEmpty, "Block commands must not emit deliveries")
    }

    private fun assertRecipients(
        sender: String,
        expected: List<String>,
        text: String = "This is BAD! Привет.",
        expectedText: String = text,
    ) {
        val message = ChatMessage.newBuilder()
            .setMessageId("m-${nextMessageId++}")
            .setSenderId(sender)
            .setText(text)
            .build()
        messages.pipeInput(sender, message.toByteArray())

        val record = deliveries.readKeyValue()
        val delivery = DeliveryEnvelope.parseFrom(record.value)
        assertEquals(sender, record.key)
        assertEquals(message.toBuilder().setText(expectedText).build(), delivery.message)
        // Recipient order is not part of the contract; sorting keeps duplicate
        // IDs visible to the assertion, unlike converting the result to a set.
        assertEquals(expected.sorted(), delivery.recipientIdsList.sorted())
        assertTrue(deliveries.isEmpty, "Each message must produce exactly one envelope")
    }

    companion object {
        @JvmStatic
        fun censorshipExamples(): List<Arguments> = listOf(
            Arguments.of("This is BAD!", "This is ***!"),
            Arguments.of("bad, really bad!", "***, really ***!"),
            Arguments.of("badge badger badly BADGE", "badge badger badly BADGE"),
            Arguments.of("bad", "***"),
            Arguments.of(" \tBAD...bad?!\n(bAd)—bad/bad ", " \t***...***?!\n(***)—***/*** "),
            Arguments.of("bad_bad _bad bad2 2bad bad-привет", "bad_bad _bad bad2 2bad ***-***"),
            Arguments.of("ПРИВЕТ, приветствие! ПрИвЕт.", "***, приветствие! ***."),
            Arguments.of("CAFÉ café cafétéria bad\u0301", "*** *** cafétéria bad\u0301"),
            Arguments.of("", ""),
            Arguments.of("***", "***"),
        )

        @JvmStatic
        fun invalidStopWords(): List<Arguments> {
            fun word(value: String) = StopWord.newBuilder().setWord(value).build().toByteArray()
            return listOf(
                Arguments.of("empty key", "", word("bad")),
                Arguments.of("mismatched key", "evil", word("bad")),
                Arguments.of("uppercase key", "BAD", word("bad")),
                Arguments.of("uppercase word", "bad", word("BAD")),
                Arguments.of("surrounding whitespace", "bad", word(" bad ")),
                Arguments.of("empty word", "bad", word("")),
                Arguments.of("multiple words", "bad word", word("bad word")),
                Arguments.of("punctuation", "bad!", word("bad!")),
                Arguments.of("malformed protobuf", "bad", byteArrayOf(0x80.toByte())),
            )
        }

        @JvmStatic
        fun invalidBlockCommands(): List<Arguments> {
            val valid = BlockCommand.newBuilder().setBlockedSenderId("Bob")
                .setRecipientId("Alice").setAction(BlockCommand.Action.BLOCK).build()
            return listOf(
                Arguments.of("null key", null, valid.toByteArray()),
                Arguments.of("empty key", "", valid.toByteArray()),
                Arguments.of("mismatched sender", "Степан", valid.toByteArray()),
                Arguments.of("empty sender", "Bob", valid.toBuilder().clearBlockedSenderId().build().toByteArray()),
                Arguments.of("empty recipient", "Bob", valid.toBuilder().clearRecipientId().build().toByteArray()),
                Arguments.of("unspecified action", "Bob", valid.toBuilder().clearAction().build().toByteArray()),
                Arguments.of("unknown action", "Bob", valid.toBuilder().setActionValue(99).build().toByteArray()),
                Arguments.of("tombstone", "Bob", null),
                Arguments.of("malformed protobuf", "Bob", byteArrayOf(0x80.toByte())),
            )
        }
    }
}
