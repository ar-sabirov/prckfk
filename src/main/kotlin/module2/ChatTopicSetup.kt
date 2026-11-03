package module2

import module2.proto.BlockCommand
import module2.proto.User
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringSerializer
import java.util.concurrent.TimeUnit

private data class TopicSpec(
    val name: String,
    val partitions: Int,
    val config: Map<String, String> = mapOf(TopicConfig.CLEANUP_POLICY_CONFIG to "delete"),
)

fun main(args: Array<String>) {
    if (args.isEmpty()) {
        createTopics()
        updateUsers("add", listOf("alice", "bob", "stepan"))
        return
    }

    val usage = "Usage: module2.ChatTopicSetupKt [<add|remove> <user-id> ... | <block|unblock> <recipient-id> <sender-id>]"
    when (args[0]) {
        "add", "remove" -> {
            require(args.size >= 2) { usage }
            val userIds = args.drop(1)
            require(userIds.all { it.isNotEmpty() }) { "User IDs must not be empty" }
            updateUsers(args[0], userIds)
        }
        "block", "unblock" -> {
            require(args.size == 3) { usage }
            require(args[1].isNotEmpty() && args[2].isNotEmpty()) { "Recipient and sender IDs must not be empty" }
            updateBlock(args[0], recipientId = args[1], senderId = args[2])
        }
        else -> throw IllegalArgumentException(usage)
    }
}

private fun createTopics() {
    val compacted = mapOf(TopicConfig.CLEANUP_POLICY_CONFIG to "compact")
    val topics = listOf(
        TopicSpec(ChatTopics.MESSAGES, ChatTopics.SENDER_PARTITIONS),
        TopicSpec(ChatTopics.USERS, ChatTopics.REFERENCE_PARTITIONS, compacted),
        TopicSpec(
            ChatTopics.BLOCK_COMMANDS,
            ChatTopics.SENDER_PARTITIONS,
            mapOf(TopicConfig.CLEANUP_POLICY_CONFIG to "delete", TopicConfig.RETENTION_MS_CONFIG to "-1"),
        ),
        TopicSpec(ChatTopics.STOP_WORDS, ChatTopics.REFERENCE_PARTITIONS, compacted),
        TopicSpec(ChatTopics.DELIVERIES, ChatTopics.SENDER_PARTITIONS),
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

private fun updateUsers(action: String, userIds: List<String>) {
    KafkaProducer(ChatConfig.producerProperties(), StringSerializer(), ByteArraySerializer()).use { producer ->
        for (userId in userIds) {
            val value = if (action == "add") User.newBuilder().setUserId(userId).build().toByteArray() else null
            val metadata = producer.send(ProducerRecord(ChatTopics.USERS, userId, value)).get(30, TimeUnit.SECONDS)
            println("Published user $action key=$userId topic=${metadata.topic()} offset=${metadata.offset()}")
        }
    }
}

private fun updateBlock(action: String, recipientId: String, senderId: String) {
    val command = BlockCommand.newBuilder()
        .setBlockedSenderId(senderId)
        .setRecipientId(recipientId)
        .setAction(if (action == "block") BlockCommand.Action.BLOCK else BlockCommand.Action.UNBLOCK)
        .build()
    KafkaProducer(ChatConfig.producerProperties(), StringSerializer(), ByteArraySerializer()).use { producer ->
        val metadata = producer.send(
            ProducerRecord(ChatTopics.BLOCK_COMMANDS, senderId, command.toByteArray()),
        ).get(30, TimeUnit.SECONDS)
        println(
            "Published $action recipient=$recipientId sender=$senderId key=$senderId " +
                "topic=${metadata.topic()} partition=${metadata.partition()} offset=${metadata.offset()}",
        )
    }
}
