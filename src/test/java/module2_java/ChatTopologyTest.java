package module2_java;

import com.google.protobuf.InvalidProtocolBufferException;
import module2.proto.BlockCommand;
import module2.proto.ChatMessage;
import module2.proto.DeliveryEnvelope;
import module2.proto.StopWord;
import module2.proto.User;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.errors.StreamsException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ChatTopologyTest {
    @TempDir
    Path stateDir;

    private TopologyTestDriver driver;
    private TestInputTopic<String, byte[]> users;
    private TestInputTopic<String, byte[]> messages;
    private TestInputTopic<String, byte[]> blockCommands;
    private TestInputTopic<String, byte[]> stopWords;
    private TestOutputTopic<String, byte[]> deliveries;
    private int nextMessageId = 1;

    @BeforeEach
    void setUp() {
        var properties = new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "module2-java-chat-test");
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "unused:9092");
        properties.setProperty(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        driver = new TopologyTestDriver(ChatTopology.buildChatTopology(), properties);
        users = driver.createInputTopic(ChatTopics.USERS, new StringSerializer(), new ByteArraySerializer());
        messages = driver.createInputTopic(ChatTopics.MESSAGES, new StringSerializer(), new ByteArraySerializer());
        blockCommands = driver.createInputTopic(ChatTopics.BLOCKED_USERS, new StringSerializer(), new ByteArraySerializer());
        stopWords = driver.createInputTopic(ChatTopics.STOP_WORDS, new StringSerializer(), new ByteArraySerializer());
        deliveries = driver.createOutputTopic(ChatTopics.FILTERED_MESSAGES, new StringDeserializer(), new ByteArrayDeserializer());
    }

    @AfterEach
    void tearDown() {
        if (driver != null) {
            driver.close();
        }
    }

    @Test
    void eachSenderReachesOtherRegisteredUsersInOneEnvelope() throws Exception {
        var members = List.of("Alice", "Bob", "Степан");
        members.forEach(this::register);
        assertTrue(deliveries.isEmpty());
        for (String sender : members) {
            assertRecipients(sender, members.stream().filter(member -> !member.equals(sender)).toList());
        }
    }

    @Test
    void joiningUsersReceiveWithoutSendingAndRepeatedRegistrationDoesNotDuplicateThem() throws Exception {
        register("Alice");
        assertRecipients("Bob", List.of("Alice"));
        register("Степан");
        register("Степан");
        assertTrue(deliveries.isEmpty());
        assertRecipients("Bob", List.of("Alice", "Степан"));
    }

    @Test
    void tombstonesRemoveMembershipAndRegistrationRestoresIt() throws Exception {
        List.of("Alice", "Bob", "Степан").forEach(this::register);
        users.pipeInput("Alice", null);
        users.pipeInput("Alice", null);
        users.pipeInput("absent-user", null);
        assertTrue(deliveries.isEmpty());
        assertRecipients("Bob", List.of("Степан"));
        register("Alice");
        assertRecipients("Bob", List.of("Alice", "Степан"));
    }

    @Test
    void emptyRegistryAndSenderOnlyRegistryStillEmitOneEnvelope() throws Exception {
        assertRecipients("Bob", List.of());
        register("Bob");
        assertRecipients("Bob", List.of());
    }

    @Test
    void blockingIsDirectionalAndUnblockingAffectsSubsequentMessages() throws Exception {
        List.of("Alice", "Bob", "Степан").forEach(this::register);
        assertRecipients("Bob", List.of("Alice", "Степан"));
        updateBlock("Alice", "Bob", BlockCommand.Action.BLOCK);
        assertRecipients("Bob", List.of("Степан"));
        assertRecipients("Alice", List.of("Bob", "Степан"));
        assertRecipients("Степан", List.of("Alice", "Bob"));
        updateBlock("Alice", "Bob", BlockCommand.Action.UNBLOCK);
        assertRecipients("Bob", List.of("Alice", "Степан"));
    }

    @Test
    void repeatedCommandsHaveSetSemanticsAndUnblockingPreservesOtherBlockers() throws Exception {
        List.of("Alice", "Bob", "Степан").forEach(this::register);
        updateBlock("Alice", "Bob", BlockCommand.Action.UNBLOCK);
        assertRecipients("Bob", List.of("Alice", "Степан"));
        updateBlock("Alice", "Bob", BlockCommand.Action.BLOCK);
        updateBlock("Alice", "Bob", BlockCommand.Action.BLOCK);
        assertRecipients("Bob", List.of("Степан"));
        updateBlock("Степан", "Bob", BlockCommand.Action.BLOCK);
        updateBlock("Степан", "Bob", BlockCommand.Action.BLOCK);
        assertRecipients("Bob", List.of());
        updateBlock("Alice", "Bob", BlockCommand.Action.UNBLOCK);
        updateBlock("Alice", "Bob", BlockCommand.Action.UNBLOCK);
        updateBlock("absent-user", "Bob", BlockCommand.Action.UNBLOCK);
        assertRecipients("Bob", List.of("Alice"));
        updateBlock("Степан", "Bob", BlockCommand.Action.UNBLOCK);
        updateBlock("Степан", "Bob", BlockCommand.Action.UNBLOCK);
        assertRecipients("Bob", List.of("Alice", "Степан"));
        updateBlock("Alice", "Bob", BlockCommand.Action.BLOCK);
        assertRecipients("Bob", List.of("Степан"));
    }

    @Test
    void eachSenderHasIndependentInverseBlockingIndex() throws Exception {
        List.of("Alice", "Bob", "Степан").forEach(this::register);
        updateBlock("Alice", "Bob", BlockCommand.Action.BLOCK);
        updateBlock("Alice", "Степан", BlockCommand.Action.BLOCK);
        updateBlock("Bob", "Степан", BlockCommand.Action.BLOCK);
        assertRecipients("Bob", List.of("Степан"));
        assertRecipients("Степан", List.of());
        assertRecipients("Alice", List.of("Bob", "Степан"));
        updateBlock("Alice", "Bob", BlockCommand.Action.UNBLOCK);
        assertRecipients("Bob", List.of("Alice", "Степан"));
        assertRecipients("Степан", List.of());
    }

    @Test
    void blockingIsIndependentOfMembershipChanges() throws Exception {
        updateBlock("Alice", "Bob", BlockCommand.Action.BLOCK);
        List.of("Alice", "Bob", "Степан").forEach(this::register);
        assertRecipients("Bob", List.of("Степан"));
        users.pipeInput("Alice", null);
        register("Alice");
        assertRecipients("Bob", List.of("Степан"));
        updateBlock("Alice", "Bob", BlockCommand.Action.UNBLOCK);
        assertRecipients("Bob", List.of("Alice", "Степан"));
    }

    @ParameterizedTest
    @MethodSource("censorshipExamples")
    void censorshipReplacesWholeWordsAndPreservesSurroundingText(String text, String expectedText) throws Exception {
        List.of("Alice", "Bob", "Степан").forEach(this::register);
        List.of("bad", "привет", "café").forEach(this::addStopWord);
        assertRecipients("Bob", List.of("Alice", "Степан"), text, expectedText);
    }

    @Test
    void addingAndRemovingStopWordsChangesOnlySubsequentMessages() throws Exception {
        register("Alice");
        assertRecipients("Bob", List.of("Alice"), "bad evil", "bad evil");
        addStopWord(" BAD ");
        addStopWord("bad");
        assertRecipients("Bob", List.of("Alice"), "bad evil", "*** evil");
        addStopWord("evil");
        assertRecipients("Bob", List.of("Alice"), "bad evil", "*** ***");
        stopWords.pipeInput("bad", null);
        stopWords.pipeInput("bad", null);
        stopWords.pipeInput("absent", null);
        assertTrue(deliveries.isEmpty(), "Rule updates must not emit deliveries");
        assertRecipients("Bob", List.of("Alice"), "BAD evil", "BAD ***");
        stopWords.pipeInput("evil", null);
        assertTrue(deliveries.isEmpty());
        assertRecipients("Bob", List.of("Alice"), "BAD evil", "BAD evil");
        addStopWord("bad");
        assertRecipients("Bob", List.of("Alice"), "BAD evil", "*** evil");
    }

    @Test
    void everySenderUsesSameGlobalCensorshipRules() throws Exception {
        var members = List.of("Alice", "Bob", "Степан");
        members.forEach(this::register);
        addStopWord("bad");
        for (String sender : members) {
            assertRecipients(sender, members.stream().filter(member -> !member.equals(sender)).toList(),
                    "Bad badge", "*** badge");
        }
    }

    @Test
    void fullyCensoredMessagesStillEmitOneEnvelopeWithFinalEligibleRecipients() throws Exception {
        addStopWord("bad");
        assertRecipients("Bob", List.of(), "BAD", "***");
        List.of("Alice", "Bob", "Степан").forEach(this::register);
        updateBlock("Alice", "Bob", BlockCommand.Action.BLOCK);
        assertRecipients("Bob", List.of("Степан"), "BAD", "***");
        updateBlock("Степан", "Bob", BlockCommand.Action.BLOCK);
        assertRecipients("Bob", List.of(), "bad", "***");
        updateBlock("Alice", "Bob", BlockCommand.Action.UNBLOCK);
        assertRecipients("Bob", List.of("Alice"), "bad", "***");
    }

    @ParameterizedTest(name = "reject invalid stop word: {0}")
    @MethodSource("invalidStopWords")
    void invalidStopWordsFailProcessingWhenTableIsRead(String description, String key, byte[] value) {
        stopWords.pipeInput(key, value);
        var message = ChatMessage.newBuilder().setMessageId("m-1").setSenderId("Bob").setText("Hello").build();
        assertThrows(StreamsException.class, () -> messages.pipeInput("Bob", message.toByteArray()), description);
        assertTrue(deliveries.isEmpty());
    }

    @Test
    void messagesAndBlockCommandsShareStoreInOneSubtopology() {
        var subtopologies = ChatTopology.buildChatTopology().describe().subtopologies();
        assertEquals(1, subtopologies.size());
        var subtopology = subtopologies.iterator().next();
        var inputTopics = subtopology.nodes().stream().filter(TopologyDescription.Source.class::isInstance)
                .map(TopologyDescription.Source.class::cast).flatMap(source -> source.topicSet().stream())
                .collect(Collectors.toSet());
        assertEquals(Set.of(ChatTopics.MESSAGES, ChatTopics.BLOCKED_USERS), inputTopics);
        var storeUsers = subtopology.nodes().stream().filter(TopologyDescription.Processor.class::isInstance)
                .map(TopologyDescription.Processor.class::cast)
                .filter(processor -> processor.stores().contains(ChatTopology.BLOCKED_BY_STORE))
                .map(TopologyDescription.Processor::name).collect(Collectors.toSet());
        assertEquals(Set.of("apply-block-command", "select-recipients"), storeUsers);
    }

    @ParameterizedTest(name = "reject invalid block command: {0}")
    @MethodSource("invalidBlockCommands")
    void invalidBlockCommandsFailProcessing(String description, String key, byte[] value) {
        assertThrows(StreamsException.class, () -> blockCommands.pipeInput(key, value), description);
        assertTrue(deliveries.isEmpty());
    }

    @Test
    void messageSenderMustMatchKafkaKey() {
        var message = ChatMessage.newBuilder().setMessageId("m-1").setSenderId("Bob").setText("Hello").build();
        assertThrows(StreamsException.class, () -> messages.pipeInput("Alice", message.toByteArray()));
        assertTrue(deliveries.isEmpty());
    }

    @Test
    void messageIdMustNotBeEmpty() {
        var message = ChatMessage.newBuilder().setSenderId("Bob").setText("Hello").build();
        assertThrows(StreamsException.class, () -> messages.pipeInput("Bob", message.toByteArray()));
        assertTrue(deliveries.isEmpty());
    }

    @Test
    void userPayloadMustMatchRegistryKey() {
        users.pipeInput("Alice", User.newBuilder().setUserId("Степан").build().toByteArray());
        var message = ChatMessage.newBuilder().setMessageId("m-1").setSenderId("Bob").setText("Hello").build();
        assertThrows(StreamsException.class, () -> messages.pipeInput("Bob", message.toByteArray()));
        assertTrue(deliveries.isEmpty());
    }

    private void register(String userId) {
        users.pipeInput(userId, User.newBuilder().setUserId(userId).build().toByteArray());
    }

    private void addStopWord(String input) {
        String word = Censorship.normalizeStopWord(input);
        stopWords.pipeInput(word, word(word));
        assertTrue(deliveries.isEmpty(), "Rule updates must not emit deliveries");
    }

    private void updateBlock(String recipient, String sender, BlockCommand.Action action) {
        var command = BlockCommand.newBuilder().setBlockedSenderId(sender)
                .setRecipientId(recipient).setAction(action).build();
        blockCommands.pipeInput(sender, command.toByteArray());
        assertTrue(deliveries.isEmpty(), "Block commands must not emit deliveries");
    }

    private void assertRecipients(String sender, List<String> expected) throws InvalidProtocolBufferException {
        assertRecipients(sender, expected, "This is BAD! Привет.", "This is BAD! Привет.");
    }

    private void assertRecipients(String sender, List<String> expected, String text, String expectedText)
            throws InvalidProtocolBufferException {
        var message = ChatMessage.newBuilder().setMessageId("m-" + nextMessageId++)
                .setSenderId(sender).setText(text).build();
        messages.pipeInput(sender, message.toByteArray());
        var record = deliveries.readKeyValue();
        var delivery = DeliveryEnvelope.parseFrom(record.value);
        assertEquals(sender, record.key);
        assertEquals(message.toBuilder().setText(expectedText).build(), delivery.getMessage());
        // Sort to ignore recipient order while still detecting duplicate IDs.
        assertEquals(expected.stream().sorted().toList(), delivery.getRecipientIdsList().stream().sorted().toList());
        assertTrue(deliveries.isEmpty(), "Each message must produce exactly one envelope");
    }

    static List<Arguments> censorshipExamples() {
        return List.of(
                Arguments.of("This is BAD!", "This is ***!"),
                Arguments.of("bad, really bad!", "***, really ***!"),
                Arguments.of("badge badger badly BADGE", "badge badger badly BADGE"),
                Arguments.of("bad", "***"),
                Arguments.of(" \tBAD...bad?!\n(bAd)—bad/bad ", " \t***...***?!\n(***)—***/*** "),
                Arguments.of("bad_bad _bad bad2 2bad bad-привет", "bad_bad _bad bad2 2bad ***-***"),
                Arguments.of("ПРИВЕТ, приветствие! ПрИвЕт.", "***, приветствие! ***."),
                Arguments.of("CAFÉ café cafétéria bad\u0301", "*** *** cafétéria bad\u0301"),
                Arguments.of("", ""), Arguments.of("***", "***"));
    }

    private static byte[] word(String value) {
        return StopWord.newBuilder().setWord(value).build().toByteArray();
    }

    static List<Arguments> invalidStopWords() {
        return List.of(
                Arguments.of("empty key", "", word("bad")),
                Arguments.of("mismatched key", "evil", word("bad")),
                Arguments.of("uppercase key", "BAD", word("bad")),
                Arguments.of("uppercase word", "bad", word("BAD")),
                Arguments.of("surrounding whitespace", "bad", word(" bad ")),
                Arguments.of("empty word", "bad", word("")),
                Arguments.of("multiple words", "bad word", word("bad word")),
                Arguments.of("punctuation", "bad!", word("bad!")),
                Arguments.of("malformed protobuf", "bad", new byte[]{(byte) 0x80}));
    }

    static List<Arguments> invalidBlockCommands() {
        var valid = BlockCommand.newBuilder().setBlockedSenderId("Bob")
                .setRecipientId("Alice").setAction(BlockCommand.Action.BLOCK).build();
        return List.of(
                Arguments.of("null key", null, valid.toByteArray()),
                Arguments.of("empty key", "", valid.toByteArray()),
                Arguments.of("mismatched sender", "Степан", valid.toByteArray()),
                Arguments.of("empty sender", "Bob", valid.toBuilder().clearBlockedSenderId().build().toByteArray()),
                Arguments.of("empty recipient", "Bob", valid.toBuilder().clearRecipientId().build().toByteArray()),
                Arguments.of("unspecified action", "Bob", valid.toBuilder().clearAction().build().toByteArray()),
                Arguments.of("unknown action", "Bob", valid.toBuilder().setActionValue(99).build().toByteArray()),
                Arguments.of("tombstone", "Bob", null),
                Arguments.of("malformed protobuf", "Bob", new byte[]{(byte) 0x80}));
    }
}
