package module2_java;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class ChatTopicSetup {
    private ChatTopicSetup() {}

    private record TopicSpec(String name, int partitions, Map<String, String> config) {
        TopicSpec(String name, int partitions) {
            this(name, partitions, Map.of(TopicConfig.CLEANUP_POLICY_CONFIG, "delete"));
        }
    }

    public static void main(String[] args) throws InterruptedException, ExecutionException, TimeoutException {
        createTopics();
    }

    private static void createTopics() throws InterruptedException, ExecutionException, TimeoutException {
        var compacted = Map.of(TopicConfig.CLEANUP_POLICY_CONFIG, "compact");
        List<TopicSpec> topics = List.of(
                new TopicSpec(ChatTopics.MESSAGES, ChatTopics.SENDER_PARTITIONS),
                new TopicSpec(ChatTopics.USERS, ChatTopics.REFERENCE_PARTITIONS, compacted),
                new TopicSpec(ChatTopics.BLOCKED_USERS, ChatTopics.SENDER_PARTITIONS,
                        Map.of(TopicConfig.CLEANUP_POLICY_CONFIG, "delete", TopicConfig.RETENTION_MS_CONFIG, "-1")),
                new TopicSpec(ChatTopics.STOP_WORDS, ChatTopics.REFERENCE_PARTITIONS, compacted),
                new TopicSpec(ChatTopics.FILTERED_MESSAGES, ChatTopics.SENDER_PARTITIONS));
        var properties = ChatConfig.clientProperties();
        properties.setProperty(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "30000");
        try (var admin = AdminClient.create(properties)) {
            var existing = admin.listTopics().names().get(30, TimeUnit.SECONDS);
            var missing = topics.stream().filter(spec -> !existing.contains(spec.name()))
                    .map(spec -> new NewTopic(spec.name(), spec.partitions(), (short) 1).configs(spec.config())).toList();
            if (!missing.isEmpty()) {
                admin.createTopics(missing).all().get(30, TimeUnit.SECONDS);
            }

            // Wrong partition counts break state locality; report rather than change them.
            var descriptions = admin.describeTopics(topics.stream().map(TopicSpec::name).toList())
                    .allTopicNames().get(30, TimeUnit.SECONDS);
            Map<TopicSpec, ConfigResource> resources = new LinkedHashMap<>();
            topics.forEach(spec -> resources.put(spec, new ConfigResource(ConfigResource.Type.TOPIC, spec.name())));
            var configs = admin.describeConfigs(resources.values()).all().get(30, TimeUnit.SECONDS);
            for (TopicSpec spec : topics) {
                int actualPartitions = descriptions.get(spec.name()).partitions().size();
                if (actualPartitions != spec.partitions()) {
                    throw new IllegalStateException(spec.name() + ": expected " + spec.partitions()
                            + " partitions, found " + actualPartitions);
                }
                for (var entry : spec.config().entrySet()) {
                    var config = configs.get(resources.get(spec)).get(entry.getKey());
                    String actual = config == null ? null : config.value();
                    if (!entry.getValue().equals(actual)) {
                        throw new IllegalStateException(spec.name() + ": expected " + entry.getKey() + "="
                                + entry.getValue() + ", found " + actual);
                    }
                }
                System.out.printf("Ready topic=%s partitions=%d config=%s%n", spec.name(), actualPartitions, spec.config());
            }
        }
    }
}
