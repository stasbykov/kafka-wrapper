package io.github.stasbykov.infrakafka.client;

import io.github.stasbykov.infrakafka.model.KafkaMessage;
import io.github.stasbykov.infrakafka.model.KafkaMessageRequest;
import io.github.stasbykov.infrakafka.model.KafkaSendResult;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers(disabledWithoutDocker = true)
class KafkaClientIntegrationTest {

    @Container
    private static final KafkaContainer KAFKA = new KafkaContainer(
            DockerImageName.parse("apache/kafka-native:3.8.0")
    );

    @Test
    void clientShouldSendReadAndInspectMessages() throws Exception {
        String topic = createTopicName();
        createTopic(topic);

        try (KafkaClient client = KafkaClient.builder()
                .bootstrapServers(KAFKA.getBootstrapServers())
                .useIsolatedGroupId("integration")
                .consumerReadTimeout(Duration.ofSeconds(10))
                .pollTimeout(Duration.ofMillis(250))
                .build()) {

            KafkaSendResult firstResult = client.sendMessage(topic, "key-1", "value-1", Map.of("traceId", "t-1"));
            List<KafkaSendResult> batchResults = client.sendBatch(topic, List.of(
                    KafkaMessageRequest.builder("value-2").key("key-2").headers(Map.of("traceId", "t-2")).build(),
                    KafkaMessageRequest.builder("value-3").key("key-3").build()
            ));

            assertEquals(topic, firstResult.topic());
            assertEquals(2, batchResults.size());

            KafkaMessage headerMatch = client.findFirstByHeader(topic, "traceId", "t-2", Duration.ofSeconds(10));
            assertNotNull(headerMatch);
            assertEquals("key-2", headerMatch.key());
            assertEquals("value-2", headerMatch.value());

            String historicValue = client.getMessage(topic, "key-1");
            assertEquals("value-1", historicValue);

            Map<Integer, Long> endOffsets = client.getEndOffsets(topic);
            assertFalse(endOffsets.isEmpty());
            assertTrue(endOffsets.values().stream().allMatch(offset -> offset != null && offset >= 3L));
        }
    }

    @Test
    void pollShouldReturnOnlyNewMessagesFromCurrentTail() throws Exception {
        String topic = createTopicName();
        createTopic(topic);

        try (KafkaClient producerClient = KafkaClient.builder()
                .bootstrapServers(KAFKA.getBootstrapServers())
                .useIsolatedGroupId("producer")
                .build();
             KafkaClient consumerClient = KafkaClient.builder()
                     .bootstrapServers(KAFKA.getBootstrapServers())
                     .useIsolatedGroupId("consumer")
                     .consumerReadTimeout(Duration.ofSeconds(5))
                     .pollTimeout(Duration.ofMillis(200))
                     .build()) {

            producerClient.sendMessage(topic, "old-key", "old-value");

            List<KafkaMessage> noHistoricMessages = consumerClient.poll(
                    topic,
                    10,
                    Duration.ofMillis(600),
                    Duration.ofMillis(200)
            );
            assertTrue(noHistoricMessages.isEmpty());

            producerClient.sendMessage(topic, "new-key", "new-value");

            List<KafkaMessage> newMessages = consumerClient.poll(
                    topic,
                    10,
                    Duration.ofSeconds(5),
                    Duration.ofMillis(200)
            );

            assertEquals(1, newMessages.size());
            assertEquals("new-key", newMessages.getFirst().key());
            assertEquals("new-value", newMessages.getFirst().value());
        }
    }

    private static void createTopic(String topic) throws Exception {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", KAFKA.getBootstrapServers());
        try (AdminClient adminClient = AdminClient.create(properties)) {
            adminClient.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get();
        }
    }

    private static String createTopicName() {
        return "test-" + UUID.randomUUID();
    }
}
