package io.github.stasbykov.infrakafka.producer;

import io.github.stasbykov.infrakafka.model.KafkaMessageRequest;
import io.github.stasbykov.infrakafka.model.KafkaSendResult;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Thin wrapper over Kafka producer for string payload workflows.
 */
public final class ProducerWrapper implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ProducerWrapper.class);

    private final Properties props;
    private final Duration producerSendTimeout;
    private final Duration closeTimeout;
    private final Object producerLock = new Object();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private volatile Producer<String, String> producer;

    /**
     * Creates a lazily initialized producer wrapper.
     *
     * @param props Kafka producer properties
     * @param producerSendTimeout default send timeout
     * @param closeTimeout graceful close timeout
     */
    public ProducerWrapper(Properties props, Duration producerSendTimeout, Duration closeTimeout) {
        this.props = copyOf(requireNonNull(props, "props"));
        this.producerSendTimeout = requirePositiveOrZero(producerSendTimeout, "producerSendTimeout");
        this.closeTimeout = requirePositiveOrZero(closeTimeout, "closeTimeout");
    }

    /**
     * @return default timeout used by send operations when not specified explicitly
     */
    public Duration defaultSendTimeout() {
        return producerSendTimeout;
    }

    /**
     * Sends a record without key or headers.
     */
    public KafkaSendResult send(String topic, String message) {
        return send(topic, null, null, null, message, null, producerSendTimeout);
    }

    /**
     * Sends a record with the provided key.
     */
    public KafkaSendResult send(String topic, String key, String message) {
        return send(topic, null, null, key, message, null, producerSendTimeout);
    }

    /**
     * Sends a record with the provided key and timeout.
     */
    public KafkaSendResult send(String topic, String key, String message, Duration sendTimeout) {
        return send(topic, null, null, key, message, null, sendTimeout);
    }

    /**
     * Sends a single record and waits for broker acknowledgement.
     *
     * @return send result metadata
     */
    public KafkaSendResult send(
            String topic,
            Integer partition,
            Long timestamp,
            String key,
            String message,
            Map<String, String> headers,
            Duration sendTimeout
    ) {
        ensureOpen();
        requireNonBlank(topic, "topic");
        requireNonNull(message, "message");

        Duration timeout = sendTimeout == null ? producerSendTimeout : requirePositiveOrZero(sendTimeout, "sendTimeout");
        ProducerRecord<String, String> record = buildRecord(topic, partition, timestamp, key, message, headers);

        try {
            RecordMetadata metadata = getOrCreateProducer()
                    .send(record)
                    .get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            log.info(
                    "Kafka record sent. topic={}, partition={}, offset={}, key={}",
                    metadata.topic(),
                    metadata.partition(),
                    metadata.offset(),
                    key
            );
            return KafkaSendResult.from(metadata);
        } catch (Exception exception) {
            throw new IllegalStateException(
                    "Failed to send Kafka record to topic '%s' with key '%s'".formatted(topic, key),
                    exception
            );
        }
    }

    /**
     * Sends a batch of messages using producer futures and then waits for all results.
     *
     * @param topic target topic
     * @param messages batch messages
     * @return send results in the same order as input
     */
    public List<KafkaSendResult> sendBatch(String topic, List<KafkaMessageRequest> messages) {
        ensureOpen();
        requireNonBlank(topic, "topic");
        requireNonNull(messages, "messages");

        Producer<String, String> currentProducer = getOrCreateProducer();
        List<PendingSend> pendingSends = new ArrayList<>(messages.size());
        for (KafkaMessageRequest message : messages) {
            KafkaMessageRequest current = requireNonNull(message, "message");
            Duration timeout = current.sendTimeout() == null
                    ? producerSendTimeout
                    : requirePositiveOrZero(current.sendTimeout(), "sendTimeout");
            ProducerRecord<String, String> record = buildRecord(
                    topic,
                    current.partition(),
                    current.timestamp(),
                    current.key(),
                    current.value(),
                    current.headers()
            );
            pendingSends.add(new PendingSend(current.key(), timeout, currentProducer.send(record)));
        }

        List<KafkaSendResult> results = new ArrayList<>(pendingSends.size());
        try {
            for (PendingSend pendingSend : pendingSends) {
                RecordMetadata metadata = pendingSend.future().get(pendingSend.timeout().toMillis(), TimeUnit.MILLISECONDS);
                log.info(
                        "Kafka record sent. topic={}, partition={}, offset={}, key={}",
                        metadata.topic(),
                        metadata.partition(),
                        metadata.offset(),
                        pendingSend.key()
                );
                results.add(KafkaSendResult.from(metadata));
            }
            return results;
        } catch (Exception exception) {
            throw new IllegalStateException(
                    "Failed to send Kafka batch to topic '%s'".formatted(topic),
                    exception
            );
        }
    }

    /**
     * Flushes and closes the underlying producer once.
     */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        synchronized (producerLock) {
            if (producer == null) {
                return;
            }
            try {
                producer.flush();
                producer.close(closeTimeout);
            } finally {
                producer = null;
            }
        }
    }

    private ProducerRecord<String, String> buildRecord(
            String topic,
            Integer partition,
            Long timestamp,
            String key,
            String message,
            Map<String, String> headers
    ) {
        RecordHeaders recordHeaders = new RecordHeaders();
        if (headers != null) {
            for (Map.Entry<String, String> entry : headers.entrySet()) {
                recordHeaders.add(header(entry.getKey(), entry.getValue()));
            }
        }
        return new ProducerRecord<>(topic, partition, timestamp, key, message, recordHeaders);
    }

    private Header header(String key, String value) {
        return new org.apache.kafka.common.header.internals.RecordHeader(
                requireNonBlank(key, "headerKey"),
                value == null ? null : value.getBytes(StandardCharsets.UTF_8)
        );
    }

    private Producer<String, String> getOrCreateProducer() {
        Producer<String, String> currentProducer = producer;
        if (currentProducer != null) {
            return currentProducer;
        }

        synchronized (producerLock) {
            if (producer == null) {
                ensureOpen();
                producer = new KafkaProducer<>(copyOf(props));
            }
            return producer;
        }
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("ProducerWrapper is already closed");
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

    private record PendingSend(String key, Duration timeout, Future<RecordMetadata> future) {
    }
}
