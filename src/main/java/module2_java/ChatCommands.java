package module2_java;

import module2.proto.BlockCommand;
import module2.proto.ChatMessage;
import module2.proto.StopWord;
import module2.proto.User;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

public final class ChatCommands {
    public static final String CHAT_COMMAND_HELP = """
            Commands:
              add <user-id> ...
              remove <user-id> ...
              block <recipient-id> <sender-id>
              unblock <recipient-id> <sender-id>
              add-stop-word <word> ...
              remove-stop-word <word> ...
              send <sender-id> <message-id> <text>
              help
            Quote arguments containing spaces in your shell.
            Kafka acknowledgement does not mean Streams has applied the update yet.
            """.stripTrailing();

    private final Producer<String, byte[]> producer;
    private final Consumer<String> output;

    public ChatCommands(Producer<String, byte[]> producer) {
        this(producer, System.out::println);
    }

    public ChatCommands(Producer<String, byte[]> producer, Consumer<String> output) {
        this.producer = producer;
        this.output = output;
    }

    public static void main(String[] args) throws InterruptedException, ExecutionException, TimeoutException {
        if (args.length == 0 || (args.length == 1 && args[0].equals("help"))) {
            System.out.println(CHAT_COMMAND_HELP);
            return;
        }
        try (var producer = new KafkaProducer<>(ChatConfig.producerProperties(),
                new StringSerializer(), new ByteArraySerializer())) {
            new ChatCommands(producer).execute(List.of(args));
        }
    }

    public void execute(List<String> args) throws InterruptedException, ExecutionException, TimeoutException {
        require(!args.isEmpty(), CHAT_COMMAND_HELP);
        String action = args.getFirst();
        List<String> values = args.subList(1, args.size());
        // Validate the whole command before publishing any of its records.
        List<ProducerRecord<String, byte[]>> records = new ArrayList<>();
        switch (action) {
            case "send" -> {
                require(values.size() == 3 && !values.get(0).isEmpty() && !values.get(1).isEmpty(),
                        "Usage: send <sender-id> <message-id> <text>");
                ChatMessage message = ChatMessage.newBuilder().setSenderId(values.get(0))
                        .setMessageId(values.get(1)).setText(values.get(2)).build();
                records.add(new ProducerRecord<>(ChatTopics.MESSAGES, message.getSenderId(), message.toByteArray()));
            }
            case "add", "remove" -> {
                require(!values.isEmpty() && values.stream().noneMatch(String::isEmpty),
                        "Usage: " + action + " <user-id> ...");
                for (String userId : values) {
                    byte[] value = action.equals("add") ? User.newBuilder().setUserId(userId).build().toByteArray() : null;
                    records.add(new ProducerRecord<>(ChatTopics.USERS, userId, value));
                }
            }
            case "block", "unblock" -> {
                require(values.size() == 2 && values.stream().noneMatch(String::isEmpty),
                        "Usage: " + action + " <recipient-id> <sender-id>");
                BlockCommand command = BlockCommand.newBuilder().setRecipientId(values.get(0))
                        .setBlockedSenderId(values.get(1))
                        .setAction(action.equals("block") ? BlockCommand.Action.BLOCK : BlockCommand.Action.UNBLOCK)
                        .build();
                records.add(new ProducerRecord<>(ChatTopics.BLOCKED_USERS, command.getBlockedSenderId(), command.toByteArray()));
            }
            case "add-stop-word", "remove-stop-word" -> {
                require(!values.isEmpty(), "Usage: " + action + " <word> ...");
                for (String input : values) {
                    String word = Censorship.normalizeStopWord(input);
                    byte[] value = action.equals("add-stop-word")
                            ? StopWord.newBuilder().setWord(word).build().toByteArray() : null;
                    records.add(new ProducerRecord<>(ChatTopics.STOP_WORDS, word, value));
                }
            }
            default -> throw new IllegalArgumentException(
                    "Unknown command: " + action + ". Run with help for available commands.");
        }
        for (ProducerRecord<String, byte[]> record : records) {
            var metadata = producer.send(record).get(30, TimeUnit.SECONDS);
            String messageInfo = action.equals("send") ? " messageId=" + values.get(1) : "";
            output.accept("Published " + action + " key=" + record.key() + messageInfo + " topic=" + metadata.topic()
                    + " partition=" + metadata.partition() + " offset=" + metadata.offset());
        }
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}
