package io.github.stasbykov.infrakafka.client;

import io.github.stasbykov.infrakafka.config.KafkaAutoOffsetReset;
import io.github.stasbykov.infrakafka.consumer.ConsumerWrapper;
import io.github.stasbykov.infrakafka.deserializer.StringDeserializer;
import io.github.stasbykov.infrakafka.producer.ProducerWrapper;
import io.github.stasbykov.infrakafka.serializer.StringSerializer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class KafkaClientBuilderTest {

    @Test
    void buildShouldFailWithoutBootstrapServers() {
        IllegalStateException exception = assertThrows(
                IllegalStateException.class,
                () -> KafkaClient.builder().build()
        );

        assertTrue(exception.getMessage().contains("bootstrapServers"));
    }

    @Test
    void buildShouldPopulateProducerAndConsumerProperties() throws Exception {
        KafkaClient client = KafkaClient.builder()
                .bootstrapServers("localhost:9092")
                .groupId("test-group")
                .securityProtocol("SSL")
                .sslKeyStoreLocation("/tmp/client.keystore")
                .sslKeyStorePassword("secret1")
                .sslTrustStoreLocation("/tmp/client.truststore")
                .sslTrustStorePassword("secret2")
                .producerSendTimeout(Duration.ofSeconds(9))
                .producerCloseTimeout(Duration.ofSeconds(3))
                .consumerReadTimeout(Duration.ofSeconds(11))
                .pollTimeout(Duration.ofMillis(250))
                .enableAutoCommit(true)
                .autoOffsetReset(KafkaAutoOffsetReset.LATEST)
                .keySerializer(new StringSerializer())
                .valueSerializer(new StringSerializer())
                .keyDeserializer(new StringDeserializer())
                .valueDeserializer(new StringDeserializer())
                .build();

        ProducerWrapper producerWrapper = getField(client, "producerWrapper", ProducerWrapper.class);
        ConsumerWrapper consumerWrapper = getField(client, "consumerWrapper", ConsumerWrapper.class);

        Properties producerProps = getField(producerWrapper, "props", Properties.class);
        Properties consumerProps = getField(consumerWrapper, "props", Properties.class);
        Duration producerSendTimeout = getField(producerWrapper, "producerSendTimeout", Duration.class);
        Duration producerCloseTimeout = getField(producerWrapper, "closeTimeout", Duration.class);
        Duration consumerReadTimeout = getField(consumerWrapper, "consumerReadTimeout", Duration.class);
        Duration pollTimeout = getField(consumerWrapper, "pollTimeout", Duration.class);

        assertEquals("localhost:9092", producerProps.getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals("SSL", producerProps.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
        assertEquals("/tmp/client.keystore", producerProps.getProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG));
        assertEquals("secret1", producerProps.getProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG));
        assertEquals("/tmp/client.truststore", producerProps.getProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG));
        assertEquals("secret2", producerProps.getProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG));
        assertEquals(org.apache.kafka.common.serialization.StringSerializer.class.getName(),
                producerProps.getProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
        assertEquals(org.apache.kafka.common.serialization.StringSerializer.class.getName(),
                producerProps.getProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));

        assertEquals("localhost:9092", consumerProps.getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals("test-group", consumerProps.getProperty(ConsumerConfig.GROUP_ID_CONFIG));
        assertEquals("latest", consumerProps.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
        assertEquals(true, consumerProps.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
        assertEquals(org.apache.kafka.common.serialization.StringDeserializer.class.getName(),
                consumerProps.getProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG));
        assertEquals(org.apache.kafka.common.serialization.StringDeserializer.class.getName(),
                consumerProps.getProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));

        assertEquals(Duration.ofSeconds(9), producerSendTimeout);
        assertEquals(Duration.ofSeconds(3), producerCloseTimeout);
        assertEquals(Duration.ofSeconds(11), consumerReadTimeout);
        assertEquals(Duration.ofMillis(250), pollTimeout);
    }

    @Test
    void autoOffsetResetStringOverloadShouldNormalizeAndValidateValue() throws Exception {
        KafkaClient client = KafkaClient.builder()
                .bootstrapServers("localhost:9092")
                .autoOffsetReset(" latest ")
                .build();

        ConsumerWrapper consumerWrapper = getField(client, "consumerWrapper", ConsumerWrapper.class);
        Properties consumerProps = getField(consumerWrapper, "props", Properties.class);

        assertEquals("latest", consumerProps.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> KafkaClient.builder()
                        .bootstrapServers("localhost:9092")
                        .autoOffsetReset("earli")
        );
        assertTrue(exception.getMessage().contains("KafkaAutoOffsetReset"));
    }

    @Test
    void buildShouldKeepProducerAndConsumerSpecificPropertiesSeparated() throws Exception {
        KafkaClient client = KafkaClient.builder()
                .bootstrapServers("localhost:9092")
                .property(CommonClientConfigs.CLIENT_ID_CONFIG, "shared-client")
                .producerProperty(ProducerConfig.ACKS_CONFIG, "all")
                .consumerProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "25")
                .keySerializerClass(org.apache.kafka.common.serialization.StringSerializer.class)
                .valueSerializerClass(org.apache.kafka.common.serialization.StringSerializer.class)
                .keyDeserializerClass(org.apache.kafka.common.serialization.StringDeserializer.class)
                .valueDeserializerClass(org.apache.kafka.common.serialization.StringDeserializer.class)
                .build();

        ProducerWrapper producerWrapper = getField(client, "producerWrapper", ProducerWrapper.class);
        ConsumerWrapper consumerWrapper = getField(client, "consumerWrapper", ConsumerWrapper.class);

        Properties producerProps = getField(producerWrapper, "props", Properties.class);
        Properties consumerProps = getField(consumerWrapper, "props", Properties.class);

        assertEquals("shared-client", producerProps.getProperty(CommonClientConfigs.CLIENT_ID_CONFIG));
        assertEquals("shared-client", consumerProps.getProperty(CommonClientConfigs.CLIENT_ID_CONFIG));

        assertEquals("all", producerProps.getProperty(ProducerConfig.ACKS_CONFIG));
        assertNull(consumerProps.get(ProducerConfig.ACKS_CONFIG));

        assertEquals("25", consumerProps.getProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG));
        assertNull(producerProps.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG));

        assertEquals(org.apache.kafka.common.serialization.StringSerializer.class.getName(),
                producerProps.getProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
        assertEquals(org.apache.kafka.common.serialization.StringSerializer.class.getName(),
                producerProps.getProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
        assertNull(producerProps.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG));
        assertNull(producerProps.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));

        assertEquals(org.apache.kafka.common.serialization.StringDeserializer.class.getName(),
                consumerProps.getProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG));
        assertEquals(org.apache.kafka.common.serialization.StringDeserializer.class.getName(),
                consumerProps.getProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));
        assertNull(consumerProps.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
        assertNull(consumerProps.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
    }

    @Test
    void fromConfigShouldReadSystemProperties() throws Exception {
        String prefix = "qa.kafka";
        System.setProperty(prefix + ".servers", "kafka1:9092,kafka2:9092");
        System.setProperty(prefix + ".keystore", "/secure/keystore.jks");
        System.setProperty(prefix + ".keystorepass", "key-pass");
        System.setProperty(prefix + ".truststore", "/secure/truststore.jks");
        System.setProperty(prefix + ".truststorepass", "trust-pass");
        System.setProperty(prefix + ".security.protocol", "sasl_ssl");

        try {
            KafkaClient client = KafkaClient.builder()
                    .fromConfig(prefix)
                    .build();

            ProducerWrapper producerWrapper = getField(client, "producerWrapper", ProducerWrapper.class);
            ConsumerWrapper consumerWrapper = getField(client, "consumerWrapper", ConsumerWrapper.class);
            Properties producerProps = getField(producerWrapper, "props", Properties.class);
            Properties consumerProps = getField(consumerWrapper, "props", Properties.class);

            assertEquals("kafka1:9092,kafka2:9092", producerProps.getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));
            assertEquals("SASL_SSL", producerProps.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
            assertEquals("/secure/keystore.jks", producerProps.getProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG));
            assertEquals("/secure/truststore.jks", producerProps.getProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG));
            assertEquals("SASL_SSL", consumerProps.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
            assertNotNull(consumerProps.getProperty(ConsumerConfig.GROUP_ID_CONFIG));
        } finally {
            System.clearProperty(prefix + ".servers");
            System.clearProperty(prefix + ".keystore");
            System.clearProperty(prefix + ".keystorepass");
            System.clearProperty(prefix + ".truststore");
            System.clearProperty(prefix + ".truststorepass");
            System.clearProperty(prefix + ".security.protocol");
        }
    }

    @Test
    void useIsolatedGroupIdShouldPrefixGeneratedValue() throws Exception {
        KafkaClient client = KafkaClient.builder()
                .bootstrapServers("localhost:9092")
                .useIsolatedGroupId("suite")
                .build();

        ConsumerWrapper consumerWrapper = getField(client, "consumerWrapper", ConsumerWrapper.class);
        Properties consumerProps = getField(consumerWrapper, "props", Properties.class);

        String groupId = consumerProps.getProperty(ConsumerConfig.GROUP_ID_CONFIG);
        assertNotNull(groupId);
        assertTrue(groupId.startsWith("suite-"));
        assertFalse(groupId.equals("suite-"));
    }

    private static <T> T getField(Object target, String fieldName, Class<T> fieldType) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return fieldType.cast(field.get(target));
    }
}
