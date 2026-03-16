package io.github.stasbykov.infrakafka.consumer;

import io.github.stasbykov.infrakafka.model.KafkaMessage;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Thin wrapper over Kafka consumer for history reads, tail polling and managed subscriptions.
 * Managed consumer operations are serialized and intended for single logical client usage.
 */
public final class ConsumerWrapper implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ConsumerWrapper.class);

    private final Properties props;
    private final Duration pollTimeout;
    private final Duration consumerReadTimeout;
    private final Object managedConsumerLock = new Object();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private volatile Consumer<String, String> managedConsumer;

    /**
     * Creates a consumer wrapper with lazily initialized managed consumer.
     *
     * @param props Kafka consumer properties
     * @param pollTimeout default poll timeout
     * @param consumerReadTimeout default total read timeout
     */
    public ConsumerWrapper(Properties props, Duration pollTimeout, Duration consumerReadTimeout) {
        this.props = copyOf(requireNonNull(props, "props"));
        this.pollTimeout = requirePositiveOrZero(pollTimeout, "pollTimeout");
        this.consumerReadTimeout = requirePositiveOrZero(consumerReadTimeout, "consumerReadTimeout");
    }

    /**
     * @return default poll timeout used by read operations
     */
    public Duration defaultPollTimeout() {
        return pollTimeout;
    }

    /**
     * Reads the first matching record by key from topic history.
     */
    public KafkaMessage findFirstRecordByKey(String topic, String key) {
        return findFirstRecordByKey(topic, key, consumerReadTimeout, pollTimeout);
    }

    /**
     * Reads the first matching record by key from topic history with explicit timeouts.
     */
    public KafkaMessage findFirstRecordByKey(String topic, String key, Duration readTimeout, Duration pollTimeout) {
        return findFirstRecordByKey(topic, key, readTimeout, pollTimeout, OffsetStartMode.EARLIEST, 0);
    }

    /**
     * Reads the first matching record by key using the provided offset start mode.
     */
    public KafkaMessage findFirstRecordByKey(
            String topic,
            String key,
            Duration readTimeout,
            Duration pollTimeout,
            OffsetStartMode offsetStartMode,
            long lookbackRecords
    ) {
        return findFirstRecord(topic, message -> Objects.equals(key, message.key()), readTimeout, pollTimeout, offsetStartMode, lookbackRecords);
    }

    /**
     * Reads the first record whose header matches the provided value.
     */
    public KafkaMessage findFirstByHeader(String topic, String headerName, String headerValue, Duration readTimeout) {
        return findFirstRecord(
                topic,
                message -> Objects.equals(headerValue, message.headerAsString(headerName)),
                readTimeout,
                pollTimeout,
                OffsetStartMode.EARLIEST,
                0
        );
    }

    /**
     * Reads all matching records by header up to {@code maxMessages}.
     */
    public List<KafkaMessage> findAllByHeader(
            String topic,
            String headerName,
            String headerValue,
            int maxMessages,
            Duration readTimeout
    ) {
        return read(topic, maxMessages, readTimeout, pollTimeout, OffsetStartMode.EARLIEST, 0, message ->
                Objects.equals(headerValue, message.headerAsString(headerName)));
    }

    /**
     * Waits for the next message appearing at the tail of the topic.
     */
    public KafkaMessage waitForMessage(String topic, Duration readTimeout) {
        return waitForMessage(topic, message -> true, readTimeout);
    }

    /**
     * Waits for the next message at the tail of the topic that matches the predicate.
     */
    public KafkaMessage waitForMessage(String topic, Predicate<KafkaMessage> predicate, Duration readTimeout) {
        return findFirstRecord(topic, predicate, readTimeout, pollTimeout, OffsetStartMode.LATEST, 0);
    }

    /**
     * Waits for the next {@code count} messages appearing at the topic tail.
     */
    public List<KafkaMessage> waitForMessages(String topic, int count, Duration readTimeout) {
        return waitForMessages(topic, message -> true, count, readTimeout);
    }

    /**
     * Waits for the next {@code count} messages appearing at the topic tail that match the predicate.
     */
    public List<KafkaMessage> waitForMessages(
            String topic,
            Predicate<KafkaMessage> predicate,
            int count,
            Duration readTimeout
    ) {
        return read(topic, count, readTimeout, pollTimeout, OffsetStartMode.LATEST, 0, predicate);
    }

    /**
     * Polls only new records from the current end of the topic.
     */
    public List<KafkaMessage> poll(String topic) {
        return poll(topic, Integer.MAX_VALUE, consumerReadTimeout, pollTimeout);
    }

    /**
     * Polls only new records from the current end of the topic with explicit limits and timeouts.
     */
    public List<KafkaMessage> poll(String topic, int maxMessages, Duration readTimeout, Duration pollTimeout) {
        return read(topic, maxMessages, readTimeout, pollTimeout, OffsetStartMode.LATEST, 0);
    }

    /**
     * Reads messages starting from the requested offset mode.
     */
    public List<KafkaMessage> read(
            String topic,
            int maxMessages,
            Duration readTimeout,
            Duration pollTimeout,
            OffsetStartMode offsetStartMode,
            long lookbackRecords
    ) {
        return read(topic, maxMessages, readTimeout, pollTimeout, offsetStartMode, lookbackRecords, message -> true);
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
        requireNonBlank(topic, "topic");
        requireNonNegative(offset, "offset");
        requirePositive(maxMessages, "maxMessages");

        try (Consumer<String, String> consumer = createStatelessConsumer()) {
            TopicPartition topicPartition = new TopicPartition(topic, partition);
            consumer.assign(List.of(topicPartition));
            consumer.seek(topicPartition, offset);
            return pollUntil(consumer, topic, maxMessages, readTimeout, pollTimeout, message -> true);
        }
    }

    /**
     * Reads end offsets for all partitions of the topic.
     */
    public Map<Integer, Long> getEndOffsets(String topic) {
        return getOffsets(topic, true);
    }

    /**
     * Reads beginning offsets for all partitions of the topic.
     */
    public Map<Integer, Long> getBeginningOffsets(String topic) {
        return getOffsets(topic, false);
    }

    /**
     * Reads committed offsets for the configured consumer group.
     */
    public Map<Integer, Long> getCommittedOffsets(String topic) {
        requireNonBlank(topic, "topic");
        ensureGroupConfigured("getCommittedOffsets");
        try (Consumer<String, String> consumer = createGroupedConsumer()) {
            List<TopicPartition> partitions = topicPartitions(consumer, topic);
            Set<TopicPartition> partitionSet = Set.copyOf(partitions);
            Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(partitionSet);

            Map<Integer, Long> offsets = new LinkedHashMap<>();
            for (TopicPartition partition : partitions) {
                OffsetAndMetadata metadata = committed.get(partition);
                offsets.put(partition.partition(), metadata == null ? null : metadata.offset());
            }
            return offsets;
        }
    }

    /**
     * Subscribes the managed consumer to the provided topics.
     */
    public void subscribe(Collection<String> topics) {
        requireNonNull(topics, "topics");
        if (topics.isEmpty()) {
            throw new IllegalArgumentException("topics must not be empty");
        }
        ensureGroupConfigured("subscribe");
        withManagedConsumer(consumer -> {
            consumer.subscribe(List.copyOf(topics));
            return null;
        });
    }

    /**
     * Polls records from the managed subscribed consumer.
     */
    public List<KafkaMessage> pollManaged(Duration timeout) {
        ensureGroupConfigured("pollManaged");
        Duration effectiveTimeout = timeout == null ? pollTimeout : requirePositiveOrZero(timeout, "timeout");
        return withManagedConsumer(consumer -> toMessages(consumer.poll(effectiveTimeout)));
    }

    /**
     * Commits offsets for the managed consumer.
     */
    public void commit() {
        ensureGroupConfigured("commit");
        withManagedConsumer(consumer -> {
            consumer.commitSync();
            return null;
        });
    }

    /**
     * Assigns the managed consumer to a partition and seeks to the provided offset.
     */
    public void seek(String topic, int partition, long offset) {
        requireNonBlank(topic, "topic");
        requireNonNegative(offset, "offset");
        ensureGroupConfigured("seek");
        withManagedConsumer(consumer -> {
            TopicPartition topicPartition = new TopicPartition(topic, partition);
            consumer.assign(List.of(topicPartition));
            consumer.seek(topicPartition, offset);
            return null;
        });
    }

    /**
     * Seeks the managed consumer to the beginning of all partitions of the topic.
     */
    public void seekToBeginning(String topic) {
        seekByMode(topic, OffsetStartMode.EARLIEST, 0);
    }

    /**
     * Seeks the managed consumer to the end of all partitions of the topic.
     */
    public void seekToEnd(String topic) {
        seekByMode(topic, OffsetStartMode.LATEST, 0);
    }

    /**
     * Closes the managed consumer once.
     */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        synchronized (managedConsumerLock) {
            if (managedConsumer != null) {
                managedConsumer.close();
                managedConsumer = null;
            }
        }
    }

    private KafkaMessage findFirstRecord(
            String topic,
            Predicate<KafkaMessage> predicate,
            Duration readTimeout,
            Duration pollTimeout,
            OffsetStartMode offsetStartMode,
            long lookbackRecords
    ) {
        List<KafkaMessage> messages = read(topic, 1, readTimeout, pollTimeout, offsetStartMode, lookbackRecords, predicate);
        return messages.isEmpty() ? null : messages.get(0);
    }

    private List<KafkaMessage> read(
            String topic,
            int maxMessages,
            Duration readTimeout,
            Duration pollTimeout,
            OffsetStartMode offsetStartMode,
            long lookbackRecords,
            Predicate<KafkaMessage> predicate
    ) {
        requireNonBlank(topic, "topic");
        requirePositive(maxMessages, "maxMessages");
        Duration effectiveReadTimeout = readTimeout == null ? consumerReadTimeout : requirePositiveOrZero(readTimeout, "readTimeout");
        Duration effectivePollTimeout = pollTimeout == null ? this.pollTimeout : requirePositiveOrZero(pollTimeout, "pollTimeout");
        OffsetStartMode startMode = requireNonNull(offsetStartMode, "offsetStartMode");
        Predicate<KafkaMessage> filter = predicate == null ? message -> true : predicate;

        try (Consumer<String, String> consumer = createStatelessConsumer()) {
            List<TopicPartition> partitions = topicPartitions(consumer, topic);
            consumer.assign(partitions);
            positionConsumer(consumer, partitions, startMode, lookbackRecords);
            return pollUntil(consumer, topic, maxMessages, effectiveReadTimeout, effectivePollTimeout, filter);
        }
    }

    private List<KafkaMessage> pollUntil(
            Consumer<String, String> consumer,
            String topic,
            int maxMessages,
            Duration readTimeout,
            Duration pollTimeout,
            Predicate<KafkaMessage> predicate
    ) {
        Duration effectiveReadTimeout = readTimeout == null ? consumerReadTimeout : requirePositiveOrZero(readTimeout, "readTimeout");
        long deadline = System.nanoTime() + effectiveReadTimeout.toNanos();
        List<KafkaMessage> messages = new ArrayList<>();

        while (System.nanoTime() <= deadline && messages.size() < maxMessages) {
            ConsumerRecords<String, String> records = consumer.poll(pollTimeout);
            for (ConsumerRecord<String, String> record : records.records(topic)) {
                KafkaMessage message = toMessage(record);
                if (predicate.test(message)) {
                    messages.add(message);
                    if (messages.size() >= maxMessages) {
                        return messages;
                    }
                }
            }
        }

        if (messages.isEmpty()) {
            log.info("No Kafka records matched for topic '{}' within {}", topic, effectiveReadTimeout);
        }
        return messages;
    }

    private void positionConsumer(
            Consumer<String, String> consumer,
            List<TopicPartition> partitions,
            OffsetStartMode offsetStartMode,
            long lookbackRecords
    ) {
        switch (offsetStartMode) {
            case EARLIEST -> consumer.seekToBeginning(partitions);
            case LATEST -> consumer.seekToEnd(partitions);
            case LAST_N_RECORDS -> seekToLastNRecords(consumer, partitions, lookbackRecords);
        }
    }

    private void seekToLastNRecords(Consumer<String, String> consumer, List<TopicPartition> partitions, long lookbackRecords) {
        if (lookbackRecords <= 0) {
            throw new IllegalArgumentException("lookbackRecords must be greater than 0 for LAST_N_RECORDS mode");
        }

        Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(partitions);
        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);

        for (TopicPartition partition : partitions) {
            long beginningOffset = beginningOffsets.getOrDefault(partition, 0L);
            long endOffset = endOffsets.getOrDefault(partition, beginningOffset);
            long startOffset = Math.max(beginningOffset, endOffset - lookbackRecords);
            consumer.seek(partition, startOffset);
        }
    }

    private void seekByMode(String topic, OffsetStartMode mode, long lookbackRecords) {
        requireNonBlank(topic, "topic");
        withManagedConsumer(consumer -> {
            List<TopicPartition> partitions = topicPartitions(consumer, topic);
            consumer.assign(partitions);
            positionConsumer(consumer, partitions, mode, lookbackRecords);
            return null;
        });
    }

    private Map<Integer, Long> getOffsets(String topic, boolean endOffsets) {
        requireNonBlank(topic, "topic");
        try (Consumer<String, String> consumer = createStatelessConsumer()) {
            List<TopicPartition> partitions = topicPartitions(consumer, topic);
            Map<TopicPartition, Long> offsets = endOffsets ? consumer.endOffsets(partitions) : consumer.beginningOffsets(partitions);

            Map<Integer, Long> result = new LinkedHashMap<>();
            for (TopicPartition partition : partitions) {
                result.put(partition.partition(), offsets.get(partition));
            }
            return result;
        }
    }

    private Consumer<String, String> createStatelessConsumer() {
        Properties statelessProps = copyOf(props);
        statelessProps.remove(ConsumerConfig.GROUP_ID_CONFIG);
        statelessProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return new KafkaConsumer<>(statelessProps);
    }

    private Consumer<String, String> createGroupedConsumer() {
        ensureGroupConfigured("createGroupedConsumer");
        return new KafkaConsumer<>(copyOf(props));
    }

    private Consumer<String, String> getOrCreateManagedConsumer() {
        ensureGroupConfigured("managed consumer operations");
        ensureOpen();
        Consumer<String, String> currentConsumer = managedConsumer;
        if (currentConsumer != null) {
            return currentConsumer;
        }

        synchronized (managedConsumerLock) {
            if (managedConsumer == null) {
                managedConsumer = createGroupedConsumer();
            }
            return managedConsumer;
        }
    }

    private <T> T withManagedConsumer(Function<Consumer<String, String>, T> action) {
        requireNonNull(action, "action");
        synchronized (managedConsumerLock) {
            return action.apply(getOrCreateManagedConsumer());
        }
    }

    private List<TopicPartition> topicPartitions(Consumer<String, String> consumer, String topic) {
        List<PartitionInfo> partitions = consumer.partitionsFor(topic);
        if (partitions == null || partitions.isEmpty()) {
            throw new IllegalStateException("No partitions found for topic '%s'".formatted(topic));
        }

        List<TopicPartition> topicPartitions = new ArrayList<>(partitions.size());
        for (PartitionInfo partition : partitions) {
            topicPartitions.add(new TopicPartition(partition.topic(), partition.partition()));
        }
        return topicPartitions;
    }

    private List<KafkaMessage> toMessages(ConsumerRecords<String, String> records) {
        if (records.isEmpty()) {
            return Collections.emptyList();
        }

        List<KafkaMessage> messages = new ArrayList<>(records.count());
        for (ConsumerRecord<String, String> record : records) {
            messages.add(toMessage(record));
        }
        return messages;
    }

    private KafkaMessage toMessage(ConsumerRecord<String, String> record) {
        Map<String, String> headers = new LinkedHashMap<>();
        for (Header header : record.headers()) {
            headers.put(header.key(), header.value() == null ? null : new String(header.value(), StandardCharsets.UTF_8));
        }
        return new KafkaMessage(
                record.topic(),
                record.partition(),
                record.offset(),
                record.timestamp(),
                record.key(),
                record.value(),
                headers
        );
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("ConsumerWrapper is already closed");
        }
    }

    private void ensureGroupConfigured(String operationName) {
        if (!props.containsKey(ConsumerConfig.GROUP_ID_CONFIG)) {
            throw new IllegalStateException("groupId is required for " + operationName);
        }
    }

    private static Properties copyOf(Properties source) {
        Properties properties = new Properties();
        properties.putAll(source);
        return properties;
    }

    private static String requireNonBlank(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be null or blank");
        }
        return value;
    }

    private static int requirePositive(int value, String fieldName) {
        if (value <= 0) {
            throw new IllegalArgumentException(fieldName + " must be greater than 0");
        }
        return value;
    }

    private static long requireNonNegative(long value, String fieldName) {
        if (value < 0) {
            throw new IllegalArgumentException(fieldName + " must not be negative");
        }
        return value;
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

    /**
     * Offset positioning strategies for history reads and managed seeks.
     */
    public enum OffsetStartMode {
        EARLIEST,
        LATEST,
        LAST_N_RECORDS
    }
}
