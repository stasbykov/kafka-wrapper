package io.github.stasbykov.infrakafka.model;

import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * Immutable metadata returned after a successful send operation.
 */
public final class KafkaSendResult {

    private final String topic;
    private final int partition;
    private final long offset;
    private final long timestamp;

    public KafkaSendResult(String topic, int partition, long offset, long timestamp) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.timestamp = timestamp;
    }

    /**
     * Creates a result object from Kafka record metadata.
     *
     * @param metadata Kafka metadata
     * @return send result
     */
    public static KafkaSendResult from(RecordMetadata metadata) {
        return new KafkaSendResult(
                metadata.topic(),
                metadata.partition(),
                metadata.offset(),
                metadata.timestamp()
        );
    }

    public String topic() {
        return topic;
    }

    public int partition() {
        return partition;
    }

    public long offset() {
        return offset;
    }

    public long timestamp() {
        return timestamp;
    }
}
