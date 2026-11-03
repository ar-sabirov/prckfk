package module2_java;

import module2.proto.BlockCommand;
import module2.proto.ChatMessage;
import module2.proto.StopWord;
import module2.proto.User;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ChatCommandsTest {
    private MockProducer<String, byte[]> producer() {
        return new MockProducer<>(true, null, new StringSerializer(), new ByteArraySerializer());
    }

    @Test
    void sendPublishesOneMessageWithExactSenderIdAndText() throws Exception {
        try (var producer = producer()) {
            List<String> output = new ArrayList<>();
            var commands = new ChatCommands(producer, output::add);
            commands.execute(List.of("send", "Степан", "m-1", "  Привет, BAD!\n"));
            assertEquals(1, producer.history().size());
            var record = producer.history().getFirst();
            assertEquals(ChatTopics.MESSAGES, record.topic());
            assertEquals("Степан", record.key());
            var message = ChatMessage.parseFrom(record.value());
            assertEquals("Степан", message.getSenderId());
            assertEquals("m-1", message.getMessageId());
            assertEquals("  Привет, BAD!\n", message.getText());
            assertEquals(1, output.size());
            assertTrue(output.getFirst().contains("messageId=m-1"));
        }
    }

    @Test
    void membershipCommandsPublishUsersAndTombstones() throws Exception {
        try (var producer = producer()) {
            var commands = new ChatCommands(producer, ignored -> {});
            commands.execute(List.of("add", "Alice", "Степан"));
            commands.execute(List.of("remove", "Степан"));
            var records = producer.history();
            assertEquals(List.of("Alice", "Степан", "Степан"), records.stream().map(ProducerRecord::key).toList());
            assertTrue(records.stream().allMatch(record -> record.topic().equals(ChatTopics.USERS)));
            assertEquals("Alice", User.parseFrom(records.get(0).value()).getUserId());
            assertEquals("Степан", User.parseFrom(records.get(1).value()).getUserId());
            assertNull(records.get(2).value());
        }
    }

    @Test
    void blockingCommandsUseBlockedSenderAsKey() throws Exception {
        try (var producer = producer()) {
            var commands = new ChatCommands(producer, ignored -> {});
            commands.execute(List.of("block", "Alice", "Степан"));
            commands.execute(List.of("unblock", "Alice", "Степан"));
            var records = producer.history();
            assertEquals(List.of("Степан", "Степан"), records.stream().map(ProducerRecord::key).toList());
            assertTrue(records.stream().allMatch(record -> record.topic().equals(ChatTopics.BLOCKED_USERS)));
            List<BlockCommand.Action> actions = new ArrayList<>();
            for (var record : records) {
                var block = BlockCommand.parseFrom(record.value());
                assertEquals("Степан", block.getBlockedSenderId());
                assertEquals("Alice", block.getRecipientId());
                actions.add(block.getAction());
            }
            assertEquals(List.of(BlockCommand.Action.BLOCK, BlockCommand.Action.UNBLOCK), actions);
        }
    }

    @Test
    void stopWordCommandsNormalizeAdditionsAndTombstoneKeys() throws Exception {
        try (var producer = producer()) {
            var commands = new ChatCommands(producer, ignored -> {});
            commands.execute(List.of("add-stop-word", " BAD ", "ПРИВЕТ"));
            commands.execute(List.of("remove-stop-word", " BAD "));
            var records = producer.history();
            assertEquals(List.of("bad", "привет", "bad"), records.stream().map(ProducerRecord::key).toList());
            assertTrue(records.stream().allMatch(record -> record.topic().equals(ChatTopics.STOP_WORDS)));
            assertEquals("bad", StopWord.parseFrom(records.get(0).value()).getWord());
            assertEquals("привет", StopWord.parseFrom(records.get(1).value()).getWord());
            assertNull(records.get(2).value());
        }
    }

    @Test
    void invalidArgumentsPublishNoRecordsEvenWithValidFirstArgument() {
        try (var producer = producer()) {
            var commands = new ChatCommands(producer, ignored -> {});
            List<List<String>> invalidCommands = List.of(
                    List.of(), List.of("unknown"), List.of("add"), List.of("remove"),
                    List.of("add", "Alice", ""), List.of("remove", "Alice", ""),
                    List.of("block", "Alice"), List.of("unblock", "Alice", "Bob", "Степан"),
                    List.of("block", "", "Bob"), List.of("unblock", "Alice", ""),
                    List.of("add-stop-word"), List.of("remove-stop-word"),
                    List.of("add-stop-word", "bad", "two words"), List.of("remove-stop-word", "bad", ""),
                    List.of("send"), List.of("send", "Bob", "m-1"),
                    List.of("send", "", "m-1", "hello"), List.of("send", "Bob", "", "hello"),
                    List.of("send", "Bob", "m-1", "hello", "extra"));
            for (var args : invalidCommands) {
                assertThrows(IllegalArgumentException.class, () -> commands.execute(args));
                assertTrue(producer.history().isEmpty(), "Invalid command must not partially publish: " + args);
            }
        }
    }
}
