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
        blockCommands = driver.createInputTopic(ChatTopics.BLOCK_COMMANDS, StringSerializer(), ByteArraySerializer())
        stopWords = driver.createInputTopic(ChatTopics.STOP_WORDS, StringSerializer(), ByteArraySerializer())
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
    fun `blocking is directional and unblocking affects subsequent messages`() {
        listOf("alice", "bob", "stepan").forEach(::register)
        assertRecipients("bob", listOf("alice", "stepan"))

        updateBlock("alice", "bob", BlockCommand.Action.BLOCK)
        assertRecipients("bob", listOf("stepan"))
        assertRecipients("alice", listOf("bob", "stepan"))
        assertRecipients("stepan", listOf("alice", "bob"))

        updateBlock("alice", "bob", BlockCommand.Action.UNBLOCK)
        assertRecipients("bob", listOf("alice", "stepan"))
    }

    @Test
    fun `repeated commands have set semantics and unblocking preserves other blockers`() {
        listOf("alice", "bob", "stepan").forEach(::register)
        updateBlock("alice", "bob", BlockCommand.Action.UNBLOCK)
        assertRecipients("bob", listOf("alice", "stepan"))

        updateBlock("alice", "bob", BlockCommand.Action.BLOCK)
        updateBlock("alice", "bob", BlockCommand.Action.BLOCK)
        assertRecipients("bob", listOf("stepan"))
        updateBlock("stepan", "bob", BlockCommand.Action.BLOCK)
        updateBlock("stepan", "bob", BlockCommand.Action.BLOCK)
        assertRecipients("bob", emptyList())

        updateBlock("alice", "bob", BlockCommand.Action.UNBLOCK)
        updateBlock("alice", "bob", BlockCommand.Action.UNBLOCK)
        updateBlock("absent-user", "bob", BlockCommand.Action.UNBLOCK)
        assertRecipients("bob", listOf("alice"))

        updateBlock("stepan", "bob", BlockCommand.Action.UNBLOCK)
        updateBlock("stepan", "bob", BlockCommand.Action.UNBLOCK)
        assertRecipients("bob", listOf("alice", "stepan"))
        updateBlock("alice", "bob", BlockCommand.Action.BLOCK)
        assertRecipients("bob", listOf("stepan"))
    }

    @Test
    fun `each sender has an independent inverse blocking index`() {
        listOf("alice", "bob", "stepan").forEach(::register)
        updateBlock("alice", "bob", BlockCommand.Action.BLOCK)
        updateBlock("alice", "stepan", BlockCommand.Action.BLOCK)
        updateBlock("bob", "stepan", BlockCommand.Action.BLOCK)
        assertRecipients("bob", listOf("stepan"))
        assertRecipients("stepan", emptyList())
        assertRecipients("alice", listOf("bob", "stepan"))

        updateBlock("alice", "bob", BlockCommand.Action.UNBLOCK)
        assertRecipients("bob", listOf("alice", "stepan"))
        assertRecipients("stepan", emptyList())
    }

    @Test
    fun `blocking is independent of membership changes`() {
        updateBlock("alice", "bob", BlockCommand.Action.BLOCK)
        listOf("alice", "bob", "stepan").forEach(::register)
        assertRecipients("bob", listOf("stepan"))
        users.pipeInput("alice", null)
        register("alice")
        assertRecipients("bob", listOf("stepan"))

        updateBlock("alice", "bob", BlockCommand.Action.UNBLOCK)
        assertRecipients("bob", listOf("alice", "stepan"))
    }

    @ParameterizedTest
    @MethodSource("censorshipExamples")
    fun `censorship replaces whole words and preserves surrounding text`(text: String, expectedText: String) {
        listOf("alice", "bob", "stepan").forEach(::register)
        listOf("bad", "привет", "café").forEach(::addStopWord)
        assertRecipients("bob", listOf("alice", "stepan"), text, expectedText)
    }

    @Test
    fun `adding and removing stop words changes only subsequent messages`() {
        register("alice")
        assertRecipients("bob", listOf("alice"), "bad evil", "bad evil")
        addStopWord(" BAD ")
        addStopWord("bad")
        assertRecipients("bob", listOf("alice"), "bad evil", "*** evil")
        addStopWord("evil")
        assertRecipients("bob", listOf("alice"), "bad evil", "*** ***")

        stopWords.pipeInput("bad", null)
        stopWords.pipeInput("bad", null)
        stopWords.pipeInput("absent", null)
        assertTrue(deliveries.isEmpty, "Rule updates must not emit deliveries")
        assertRecipients("bob", listOf("alice"), "BAD evil", "BAD ***")
        stopWords.pipeInput("evil", null)
        assertTrue(deliveries.isEmpty)
        assertRecipients("bob", listOf("alice"), "BAD evil", "BAD evil")
        addStopWord("bad")
        assertRecipients("bob", listOf("alice"), "BAD evil", "*** evil")
    }

    @Test
    fun `every sender uses the same global censorship rules`() {
        val members = listOf("alice", "bob", "stepan")
        members.forEach(::register)
        addStopWord("bad")
        for (sender in members) {
            assertRecipients(sender, members.filter { it != sender }, "Bad badge", "*** badge")
        }
    }

    @Test
    fun `fully censored messages still emit one envelope with final eligible recipients`() {
        addStopWord("bad")
        assertRecipients("bob", emptyList(), "BAD", "***")
        listOf("alice", "bob", "stepan").forEach(::register)
        updateBlock("alice", "bob", BlockCommand.Action.BLOCK)
        assertRecipients("bob", listOf("stepan"), "BAD", "***")
        updateBlock("stepan", "bob", BlockCommand.Action.BLOCK)
        assertRecipients("bob", emptyList(), "bad", "***")
        updateBlock("alice", "bob", BlockCommand.Action.UNBLOCK)
        assertRecipients("bob", listOf("alice"), "bad", "***")
    }

    @ParameterizedTest(name = "reject invalid stop word: {0}")
    @MethodSource("invalidStopWords")
    fun `invalid stop words fail processing when the table is read`(description: String, key: String, value: ByteArray) {
        stopWords.pipeInput(key, value)
        val message = ChatMessage.newBuilder().setMessageId("m-1").setSenderId("bob").setText("Hello").build()
        assertThrows(StreamsException::class.java, { messages.pipeInput("bob", message.toByteArray()) }, description)
        assertTrue(deliveries.isEmpty)
    }

    @Test
    fun `messages and block commands share the store in one subtopology`() {
        val subtopology = buildChatTopology().describe().subtopologies().single()
        val inputTopics = subtopology.nodes().filterIsInstance<TopologyDescription.Source>()
            .flatMap { it.topicSet() }.toSet()
        assertEquals(setOf(ChatTopics.MESSAGES, ChatTopics.BLOCK_COMMANDS), inputTopics)
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
            val valid = BlockCommand.newBuilder().setBlockedSenderId("bob")
                .setRecipientId("alice").setAction(BlockCommand.Action.BLOCK).build()
            return listOf(
                Arguments.of("null key", null, valid.toByteArray()),
                Arguments.of("empty key", "", valid.toByteArray()),
                Arguments.of("mismatched sender", "stepan", valid.toByteArray()),
                Arguments.of("empty sender", "bob", valid.toBuilder().clearBlockedSenderId().build().toByteArray()),
                Arguments.of("empty recipient", "bob", valid.toBuilder().clearRecipientId().build().toByteArray()),
                Arguments.of("unspecified action", "bob", valid.toBuilder().clearAction().build().toByteArray()),
                Arguments.of("unknown action", "bob", valid.toBuilder().setActionValue(99).build().toByteArray()),
                Arguments.of("tombstone", "bob", null),
                Arguments.of("malformed protobuf", "bob", byteArrayOf(0x80.toByte())),
            )
        }
    }
}
