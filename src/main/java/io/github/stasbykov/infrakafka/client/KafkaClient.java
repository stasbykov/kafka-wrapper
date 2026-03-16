package io.github.stasbykov.infrakafka.client;

import io.github.stasbykov.infrakafka.config.KafkaConnectionConfig;
import io.github.stasbykov.infrakafka.config.KafkaAutoOffsetReset;
import io.github.stasbykov.infrakafka.config.KafkaSecurityProtocol;
import io.github.stasbykov.infrakafka.consumer.ConsumerWrapper;
import io.github.stasbykov.infrakafka.deserializer.Deserializer;
import io.github.stasbykov.infrakafka.model.KafkaMessage;
import io.github.stasbykov.infrakafka.model.KafkaMessageRequest;
import io.github.stasbykov.infrakafka.model.KafkaSendResult;
import io.github.stasbykov.infrakafka.producer.ProducerWrapper;
import io.github.stasbykov.infrakafka.serializer.Serializer;
import org.aeonbits.owner.ConfigFactory;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;

import java.time.Duration;
import java.util.*;
import java.util.function.Predicate;

/**
 * High-level facade for common Kafka producer and consumer test workflows.
 */
public final class KafkaClient implements AutoCloseable {

    private final ProducerWrapper producerWrapper;
    private final ConsumerWrapper consumerWrapper;

    private KafkaClient(ProducerWrapper producerWrapper, ConsumerWrapper consumerWrapper) {
        this.producerWrapper = requireNonNull(producerWrapper, "producerWrapper");
        this.consumerWrapper = requireNonNull(consumerWrapper, "consumerWrapper");
    }

    /**
     * Creates a new client builder.
     *
     * @return builder instance
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Sends a message without key or headers.
     */
    public KafkaSendResult sendMessage(String topic, String message) {
        return producerWrapper.send(topic, message);
    }

    /**
     * Sends a message with key.
     */
    public KafkaSendResult sendMessage(String topic, String key, String message) {
        return producerWrapper.send(topic, key, message);
    }

    /**
     * Sends a message with key and explicit timeout.
     */
    public KafkaSendResult sendMessage(String topic, String key, String message, Duration sendTimeout) {
        return producerWrapper.send(topic, key, message, sendTimeout);
    }

    /**
     * Sends a message with key and string headers.
     */
    public KafkaSendResult sendMessage(String topic, String key, String message, Map<String, String> headers) {
        return producerWrapper.send(topic, null, null, key, message, headers, producerWrapper.defaultSendTimeout());
    }

    /**
     * Sends a message to an explicit partition with headers.
     */
    public KafkaSendResult sendMessage(
            String topic,
            Integer partition,
            String key,
            String message,
            Map<String, String> headers
    ) {
        return producerWrapper.send(topic, partition, null, key, message, headers, producerWrapper.defaultSendTimeout());
    }

    /**
     * Sends a fully customized message.
     */
    public KafkaSendResult sendMessage(
            String topic,
            Integer partition,
            Long timestamp,
            String key,
            String message,
            Map<String, String> headers,
            Duration sendTimeout
    ) {
        return producerWrapper.send(topic, partition, timestamp, key, message, headers, sendTimeout);
    }

    /**
     * Sends a batch of messages and returns results in input order.
     */
    public List<KafkaSendResult> sendBatch(String topic, List<KafkaMessageRequest> messages) {
        return producerWrapper.sendBatch(topic, messages);
    }

    /**
     * Reads the first historical message by key and returns only its value.
     */
    public String getMessage(String topic, String key) {
        KafkaMessage message = consumerWrapper.findFirstRecordByKey(topic, key);
        return message != null ? message.value() : null;
    }

    /**
     * Reads the first historical message by key with explicit timeouts and returns only its value.
     */
    public String getMessage(String topic, String key, Duration readTimeout, Duration pollTimeout) {
        KafkaMessage message = consumerWrapper.findFirstRecordByKey(topic, key, readTimeout, pollTimeout);
        return message != null ? message.value() : null;
    }

    /**
     * Reads the first message by key using the requested offset mode and returns only its value.
     */
    public String getMessage(
            String topic,
            String key,
            Duration readTimeout,
            Duration pollTimeout,
            ConsumerWrapper.OffsetStartMode offsetStartMode,
            long lookbackRecords
    ) {
        KafkaMessage message = consumerWrapper.findFirstRecordByKey(
                topic,
                key,
                readTimeout,
                pollTimeout,
                offsetStartMode,
                lookbackRecords
        );
        return message != null ? message.value() : null;
    }

    /**
     * Reads the first historical message by key including metadata.
     */
    public KafkaMessage getMessageWithAdditionalInfo(String topic, String key) {
        return consumerWrapper.findFirstRecordByKey(topic, key);
    }

    /**
     * Reads the first historical message by key including metadata with explicit timeouts.
     */
    public KafkaMessage getMessageWithAdditionalInfo(
            String topic,
            String key,
            Duration readTimeout,
            Duration pollTimeout
    ) {
        return consumerWrapper.findFirstRecordByKey(topic, key, readTimeout, pollTimeout);
    }

    /**
     * Reads the first message by key including metadata using the requested offset mode.
     */
    public KafkaMessage getMessageWithAdditionalInfo(
            String topic,
            String key,
            Duration readTimeout,
            Duration pollTimeout,
            ConsumerWrapper.OffsetStartMode offsetStartMode,
            long lookbackRecords
    ) {
        return consumerWrapper.findFirstRecordByKey(topic, key, readTimeout, pollTimeout, offsetStartMode, lookbackRecords);
    }

    /**
     * Waits for the next message from the topic tail.
     */
    public KafkaMessage waitForMessage(String topic, Duration readTimeout) {
        return consumerWrapper.waitForMessage(topic, readTimeout);
    }

    /**
     * Waits for the next message from the topic tail that matches the predicate.
     */
    public KafkaMessage waitForMessage(String topic, Predicate<KafkaMessage> predicate, Duration readTimeout) {
        return consumerWrapper.waitForMessage(topic, predicate, readTimeout);
    }

    /**
     * Waits for the next {@code count} messages from the topic tail.
     */
    public List<KafkaMessage> waitForMessages(String topic, int count, Duration readTimeout) {
        return consumerWrapper.waitForMessages(topic, count, readTimeout);
    }

    /**
     * Waits for the next {@code count} messages from the topic tail that match the predicate.
     */
    public List<KafkaMessage> waitForMessages(
            String topic,
            Predicate<KafkaMessage> predicate,
            int count,
            Duration readTimeout
    ) {
        return consumerWrapper.waitForMessages(topic, predicate, count, readTimeout);
    }

    /**
     * Polls only new records from the current topic tail.
     */
    public List<KafkaMessage> poll(String topic) {
        return consumerWrapper.poll(topic);
    }

    /**
     * Polls only new records from the current topic tail with explicit limits and timeouts.
     */
    public List<KafkaMessage> poll(String topic, int maxMessages, Duration readTimeout, Duration pollTimeout) {
        return consumerWrapper.poll(topic, maxMessages, readTimeout, pollTimeout);
    }

    /**
     * Reads historical messages from the beginning of the topic.
     */
    public List<KafkaMessage> readFromBeginning(String topic, int maxMessages, Duration readTimeout) {
        return consumerWrapper.read(
                topic,
                maxMessages,
                readTimeout,
                consumerWrapper.defaultPollTimeout(),
                ConsumerWrapper.OffsetStartMode.EARLIEST,
                0
        );
    }

    /**
     * Reads the last {@code lookbackRecords} historical messages from the topic.
     */
    public List<KafkaMessage> readFromEnd(String topic, int maxMessages, Duration readTimeout, long lookbackRecords) {
        return consumerWrapper.read(
                topic,
                maxMessages,
                readTimeout,
                consumerWrapper.defaultPollTimeout(),
                ConsumerWrapper.OffsetStartMode.LAST_N_RECORDS,
                lookbackRecords
        );
    }

    /**
     * Reads messages starting from an explicit partition offset.
     */
    public List<KafkaMessage> readFromOffset(
            String topic,
            int partition,
            long offset,
            int maxMessages,
            Duration readTimeout
    ) {
        return consumerWrapper.readFromOffset(topic, partition, offset, maxMessages, readTimeout);
    }

    /**
     * Finds the first historical message with matching header value.
     */
    public KafkaMessage findFirstByHeader(String topic, String headerName, String headerValue, Duration readTimeout) {
        return consumerWrapper.findFirstByHeader(topic, headerName, headerValue, readTimeout);
    }

    /**
     * Finds up to {@code maxMessages} historical messages with matching header value.
     */
    public List<KafkaMessage> findAllByHeader(
            String topic,
            String headerName,
            String headerValue,
            int maxMessages,
            Duration readTimeout
    ) {
        return consumerWrapper.findAllByHeader(topic, headerName, headerValue, maxMessages, readTimeout);
    }

    /**
     * Finds the first historical message by key including metadata.
     */
    public KafkaMessage findFirstByKey(String topic, String key, Duration readTimeout) {
        return consumerWrapper.findFirstRecordByKey(topic, key, readTimeout, consumerWrapper.defaultPollTimeout());
    }

    /**
     * Returns end offsets for all partitions of the topic.
     */
    public Map<Integer, Long> getEndOffsets(String topic) {
        return consumerWrapper.getEndOffsets(topic);
    }

    /**
     * Returns beginning offsets for all partitions of the topic.
     */
    public Map<Integer, Long> getBeginningOffsets(String topic) {
        return consumerWrapper.getBeginningOffsets(topic);
    }

    /**
     * Returns committed offsets for the configured consumer group.
     */
    public Map<Integer, Long> getCommittedOffsets(String topic) {
        return consumerWrapper.getCommittedOffsets(topic);
    }

    /**
     * Subscribes the managed consumer to a single topic.
     */
    public void subscribe(String topic) {
        consumerWrapper.subscribe(List.of(topic));
    }

    /**
     * Subscribes the managed consumer to multiple topics.
     */
    public void subscribe(Collection<String> topics) {
        consumerWrapper.subscribe(topics);
    }

    /**
     * Polls records from the managed subscribed consumer.
     */
    public List<KafkaMessage> pollSubscribed(Duration timeout) {
        return consumerWrapper.pollManaged(timeout);
    }

    /**
     * Commits offsets for the managed subscribed consumer.
     */
    public void commit() {
        consumerWrapper.commit();
    }

    /**
     * Seeks the managed consumer to an explicit partition offset.
     */
    public void seek(String topic, int partition, long offset) {
        consumerWrapper.seek(topic, partition, offset);
    }

    /**
     * Seeks the managed consumer to the beginning of the topic.
     */
    public void seekToBeginning(String topic) {
        consumerWrapper.seekToBeginning(topic);
    }

    /**
     * Seeks the managed consumer to the end of the topic.
     */
    public void seekToEnd(String topic) {
        consumerWrapper.seekToEnd(topic);
    }

    /**
     * Closes both producer and consumer wrappers.
     */
    @Override
    public void close() {
        producerWrapper.close();
        consumerWrapper.close();
    }

    /**
     * Builder for {@link KafkaClient}.
     */
    public static final class Builder {

        private final Properties commonProperties = new Properties();
        private final Properties producerProperties = new Properties();
        private final Properties consumerProperties = new Properties();
        private Duration consumerReadTimeout = Duration.ofSeconds(30);
        private Duration pollTimeout = Duration.ofMillis(500);
        private Duration producerSendTimeout = Duration.ofSeconds(15);
        private Duration producerCloseTimeout = Duration.ofSeconds(5);
        private KafkaAutoOffsetReset autoOffsetReset = KafkaAutoOffsetReset.EARLIEST;
        private boolean enableAutoCommit = false;
        private String groupId;
        private String keySerializer = new io.github.stasbykov.infrakafka.serializer.StringSerializer().serializerClassName();
        private String valueSerializer = new io.github.stasbykov.infrakafka.serializer.StringSerializer().serializerClassName();
        private String keyDeserializer = new io.github.stasbykov.infrakafka.deserializer.StringDeserializer().deserializerClassName();
        private String valueDeserializer = new io.github.stasbykov.infrakafka.deserializer.StringDeserializer().deserializerClassName();

        private Builder() {
        }

        /**
         * Loads Kafka connection settings from OWNER config for the given service prefix.
         *
         * @param serviceName config prefix
         * @return current builder
         */
        public Builder fromConfig(String serviceName) {
            requireNonBlank(serviceName, "serviceName");
            KafkaConnectionConfig config = ConfigFactory.create(
                    KafkaConnectionConfig.class,
                    Map.of("service", normalizedServiceName(serviceName))
            );
            return fromConfig(config);
        }

        /**
         * Applies Kafka connection settings from OWNER config object.
         *
         * @param config config source
         * @return current builder
         */
        public Builder fromConfig(KafkaConnectionConfig config) {
            requireNonNull(config, "config");
            if (isPresent(config.servers())) {
                bootstrapServers(config.servers());
            }
            if (isPresent(config.keyStore())) {
                sslKeyStoreLocation(config.keyStore());
            }
            if (isPresent(config.keyStorePass())) {
                sslKeyStorePassword(config.keyStorePass());
            }
            if (isPresent(config.trustStore())) {
                sslTrustStoreLocation(config.trustStore());
            }
            if (isPresent(config.trustStorePass())) {
                sslTrustStorePassword(config.trustStorePass());
            }
            if (isPresent(config.securityProtocol())) {
                securityProtocol(KafkaSecurityProtocol.fromExternalValue(config.securityProtocol()));
            }
            return this;
        }

        /**
         * Sets bootstrap servers.
         */
        public Builder bootstrapServers(String servers) {
            commonProperties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, requireNonBlank(servers, "bootstrapServers"));
            return this;
        }

        /**
         * Sets bootstrap servers from list.
         */
        public Builder bootstrapServers(List<String> servers) {
            if (servers == null || servers.isEmpty()) {
                throw new IllegalArgumentException("bootstrapServers must not be null or empty");
            }
            return bootstrapServers(String.join(",", servers));
        }

        /**
         * Sets raw Kafka security protocol value.
         */
        public Builder securityProtocol(String securityProtocol) {
            commonProperties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, requireNonBlank(securityProtocol, "securityProtocol"));
            return this;
        }

        /**
         * Sets typed Kafka security protocol.
         */
        public Builder securityProtocol(KafkaSecurityProtocol securityProtocol) {
            requireNonNull(securityProtocol, "securityProtocol");
            return securityProtocol(securityProtocol.name());
        }

        /**
         * Sets SSL keystore location.
         */
        public Builder sslKeyStoreLocation(String keyStoreLocation) {
            commonProperties.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, requireNonBlank(keyStoreLocation, "sslKeyStoreLocation"));
            return this;
        }

        /**
         * Sets SSL keystore password.
         */
        public Builder sslKeyStorePassword(String keyStorePassword) {
            commonProperties.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, requireNonBlank(keyStorePassword, "sslKeyStorePassword"));
            return this;
        }

        /**
         * Sets SSL truststore location.
         */
        public Builder sslTrustStoreLocation(String trustStoreLocation) {
            commonProperties.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, requireNonBlank(trustStoreLocation, "sslTrustStoreLocation"));
            return this;
        }

        /**
         * Sets SSL truststore password.
         */
        public Builder sslTrustStorePassword(String trustStorePassword) {
            commonProperties.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, requireNonBlank(trustStorePassword, "sslTrustStorePassword"));
            return this;
        }

        /**
         * Sets default total read timeout for consumer operations.
         */
        public Builder consumerReadTimeout(Duration consumerReadTimeout) {
            this.consumerReadTimeout = requirePositiveOrZero(consumerReadTimeout, "consumerReadTimeout");
            return this;
        }

        /**
         * Sets default Kafka poll timeout.
         */
        public Builder pollTimeout(Duration pollTimeout) {
            this.pollTimeout = requirePositiveOrZero(pollTimeout, "pollTimeout");
            return this;
        }

        /**
         * Sets default producer send timeout.
         */
        public Builder producerSendTimeout(Duration producerSendTimeout) {
            this.producerSendTimeout = requirePositiveOrZero(producerSendTimeout, "producerSendTimeout");
            return this;
        }

        /**
         * Sets producer close timeout.
         */
        public Builder producerCloseTimeout(Duration producerCloseTimeout) {
            this.producerCloseTimeout = requirePositiveOrZero(producerCloseTimeout, "producerCloseTimeout");
            return this;
        }

        /**
         * Sets consumer group id.
         */
        public Builder groupId(String groupId) {
            this.groupId = requireNonBlank(groupId, "groupId");
            return this;
        }

        /**
         * Generates an isolated consumer group id using the provided prefix.
         */
        public Builder useIsolatedGroupId(String prefix) {
            this.groupId = requireNonBlank(prefix, "prefix") + "-" + UUID.randomUUID();
            return this;
        }

        /**
         * Enables or disables consumer auto commit.
         */
        public Builder enableAutoCommit(boolean enableAutoCommit) {
            this.enableAutoCommit = enableAutoCommit;
            return this;
        }

        /**
         * Parses and sets {@code auto.offset.reset} from external string value.
         */
        public Builder autoOffsetReset(String autoOffsetReset) {
            return autoOffsetReset(KafkaAutoOffsetReset.fromExternalValue(autoOffsetReset));
        }

        /**
         * Sets typed {@code auto.offset.reset}.
         */
        public Builder autoOffsetReset(KafkaAutoOffsetReset autoOffsetReset) {
            this.autoOffsetReset = requireNonNull(autoOffsetReset, "autoOffsetReset");
            return this;
        }

        /**
         * Sets key serializer adapter.
         */
        public Builder keySerializer(Serializer serializer) {
            keySerializer = requireNonNull(serializer, "keySerializer").serializerClassName();
            return this;
        }

        /**
         * Sets value serializer adapter.
         */
        public Builder valueSerializer(Serializer serializer) {
            valueSerializer = requireNonNull(serializer, "valueSerializer").serializerClassName();
            return this;
        }

        /**
         * Sets key deserializer adapter.
         */
        public Builder keyDeserializer(Deserializer deserializer) {
            keyDeserializer = requireNonNull(deserializer, "keyDeserializer").deserializerClassName();
            return this;
        }

        /**
         * Sets value deserializer adapter.
         */
        public Builder valueDeserializer(Deserializer deserializer) {
            valueDeserializer = requireNonNull(deserializer, "valueDeserializer").deserializerClassName();
            return this;
        }

        /**
         * Sets key serializer class directly.
         */
        public Builder keySerializerClass(Class<?> serializerClass) {
            keySerializer = requireNonNull(serializerClass, "keySerializerClass").getName();
            return this;
        }

        /**
         * Sets value serializer class directly.
         */
        public Builder valueSerializerClass(Class<?> serializerClass) {
            valueSerializer = requireNonNull(serializerClass, "valueSerializerClass").getName();
            return this;
        }

        /**
         * Sets key deserializer class directly.
         */
        public Builder keyDeserializerClass(Class<?> deserializerClass) {
            keyDeserializer = requireNonNull(deserializerClass, "keyDeserializerClass").getName();
            return this;
        }

        /**
         * Sets value deserializer class directly.
         */
        public Builder valueDeserializerClass(Class<?> deserializerClass) {
            valueDeserializer = requireNonNull(deserializerClass, "valueDeserializerClass").getName();
            return this;
        }

        /**
         * Adds a producer-only property.
         */
        public Builder producerProperty(String key, Object value) {
            producerProperties.put(requireNonBlank(key, "producerPropertyKey"), requireNonNull(value, "producerPropertyValue"));
            return this;
        }

        /**
         * Adds a consumer-only property.
         */
        public Builder consumerProperty(String key, Object value) {
            consumerProperties.put(requireNonBlank(key, "consumerPropertyKey"), requireNonNull(value, "consumerPropertyValue"));
            return this;
        }

        /**
         * Adds a shared property copied to both producer and consumer configs.
         */
        public Builder property(String key, Object value) {
            commonProperties.put(requireNonBlank(key, "propertyKey"), requireNonNull(value, "propertyValue"));
            return this;
        }

        /**
         * Builds the client with isolated producer and consumer property sets.
         *
         * @return configured client
         */
        public KafkaClient build() {
            if (!commonProperties.containsKey(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)) {
                throw new IllegalStateException("bootstrapServers must be configured");
            }

            Properties producerProps = new Properties();
            producerProps.putAll(commonProperties);
            producerProps.putAll(producerProperties);
            producerProps.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
            producerProps.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);

            Properties consumerProps = new Properties();
            consumerProps.putAll(commonProperties);
            consumerProps.putAll(consumerProperties);
            consumerProps.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
            consumerProps.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
            consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset.externalValue());
            consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
            if (groupId != null) {
                consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            }

            ProducerWrapper producerWrapper = new ProducerWrapper(producerProps, producerSendTimeout, producerCloseTimeout);
            ConsumerWrapper consumerWrapper = new ConsumerWrapper(consumerProps, pollTimeout, consumerReadTimeout);
            return new KafkaClient(producerWrapper, consumerWrapper);
        }
    }

    private static String requireNonBlank(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be null or blank");
        }
        return value;
    }

    private static boolean isPresent(String value) {
        return value != null && !value.isBlank();
    }

    private static String normalizedServiceName(String value) {
        return value.trim().toLowerCase(Locale.ROOT);
    }

    private static Duration requirePositiveOrZero(Duration value, String fieldName) {
        if (value == null || value.isNegative()) {
            throw new IllegalArgumentException(fieldName + " must not be null or negative");
        }
        return value;
    }

    private static <T> T requireNonNull(T value, String fieldName) {
        if (value == null) {
            throw new IllegalArgumentException(fieldName + " must not be null");
        }
        return value;
    }
}
