package module2

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.config.TopicConfig
import java.util.concurrent.TimeUnit

private data class TopicSpec(
    val name: String,
    val partitions: Int,
    val config: Map<String, String> = mapOf(TopicConfig.CLEANUP_POLICY_CONFIG to "delete"),
)

fun main(args: Array<String>) {
    createTopics()
}

private fun createTopics() {
    val compacted = mapOf(TopicConfig.CLEANUP_POLICY_CONFIG to "compact")
    val topics = listOf(
        TopicSpec(ChatTopics.MESSAGES, ChatTopics.SENDER_PARTITIONS),
        TopicSpec(ChatTopics.USERS, ChatTopics.REFERENCE_PARTITIONS, compacted),
        TopicSpec(
            ChatTopics.BLOCKED_USERS,
            ChatTopics.SENDER_PARTITIONS,
            mapOf(TopicConfig.CLEANUP_POLICY_CONFIG to "delete", TopicConfig.RETENTION_MS_CONFIG to "-1"),
        ),
        TopicSpec(ChatTopics.STOP_WORDS, ChatTopics.REFERENCE_PARTITIONS, compacted),
        TopicSpec(ChatTopics.FILTERED_MESSAGES, ChatTopics.SENDER_PARTITIONS),
    )
    val properties = ChatConfig.clientProperties().apply {
        setProperty(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "30000")
    }
    AdminClient.create(properties).use { admin ->
        val existing = admin.listTopics().names().get(30, TimeUnit.SECONDS)
        val missing = topics.filter { it.name !in existing }
        if (missing.isNotEmpty()) {
            admin.createTopics(missing.map {
                NewTopic(it.name, it.partitions, 1.toShort()).configs(it.config)
            }).all().get(30, TimeUnit.SECONDS)
        }

        // An existing topic with the wrong partition count can break state locality.
        // Report the mismatch instead of silently changing an existing topic.
        val descriptions = admin.describeTopics(topics.map { it.name }).allTopicNames().get(30, TimeUnit.SECONDS)
        val resources = topics.associateWith { ConfigResource(ConfigResource.Type.TOPIC, it.name) }
        val configs = admin.describeConfigs(resources.values).all().get(30, TimeUnit.SECONDS)
        topics.forEach { spec ->
            val actualPartitions = descriptions.getValue(spec.name).partitions().size
            check(actualPartitions == spec.partitions) {
                "${spec.name}: expected ${spec.partitions} partitions, found $actualPartitions"
            }
            spec.config.forEach { (key, expected) ->
                val actual = configs.getValue(resources.getValue(spec)).get(key)?.value()
                check(actual == expected) { "${spec.name}: expected $key=$expected, found $actual" }
            }
            println("Ready topic=${spec.name} partitions=$actualPartitions config=${spec.config}")
        }
    }
}
