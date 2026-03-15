package io.github.stasbykov.infrakafka.model;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Immutable view of a consumed Kafka record with decoded string headers.
 */
public final class KafkaMessage {

    private final String topic;
    private final int partition;
    private final long offset;
    private final long timestamp;
    private final String key;
    private final String value;
    private final Map<String, String> headers;

    /**
     * Creates an immutable message representation.
     */
    public KafkaMessage(
            String topic,
            int partition,
            long offset,
            long timestamp,
            String key,
            String value,
            Map<String, String> headers
    ) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.timestamp = timestamp;
        this.key = key;
        this.value = value;
        this.headers = headers == null ? Collections.emptyMap() : Collections.unmodifiableMap(new LinkedHashMap<>(headers));
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

    public String key() {
        return key;
    }

    public String value() {
        return value;
    }

    public Map<String, String> headers() {
        return headers;
    }

    /**
     * Returns the header value decoded as UTF-8 string.
     *
     * @param name header name
     * @return header value or {@code null} when absent
     */
    public String headerAsString(String name) {
        return headers.get(name);
    }
}
