package io.github.stasbykov.infrakafka.model;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Immutable send request used for batch producer operations.
 */
public final class KafkaMessageRequest {

    private final Integer partition;
    private final Long timestamp;
    private final String key;
    private final String value;
    private final Map<String, String> headers;
    private final Duration sendTimeout;

    private KafkaMessageRequest(Builder builder) {
        this.partition = builder.partition;
        this.timestamp = builder.timestamp;
        this.key = builder.key;
        this.value = requireNonNull(builder.value, "value");
        this.headers = builder.headers == null
                ? Collections.emptyMap()
                : Collections.unmodifiableMap(new LinkedHashMap<>(builder.headers));
        this.sendTimeout = builder.sendTimeout;
    }

    /**
     * Starts building a message request for the provided value.
     *
     * @param value message payload
     * @return request builder
     */
    public static Builder builder(String value) {
        return new Builder(value);
    }

    public Integer partition() {
        return partition;
    }

    public Long timestamp() {
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

    public Duration sendTimeout() {
        return sendTimeout;
    }

    /**
     * Builder for {@link KafkaMessageRequest}.
     */
    public static final class Builder {

        private Integer partition;
        private Long timestamp;
        private String key;
        private final String value;
        private Map<String, String> headers;
        private Duration sendTimeout;

        private Builder(String value) {
            this.value = value;
        }

        /**
         * Pins the record to a partition.
         *
         * @param partition target partition
         * @return current builder
         */
        public Builder partition(Integer partition) {
            this.partition = partition;
            return this;
        }

        /**
         * Sets record timestamp.
         *
         * @param timestamp Kafka record timestamp in milliseconds
         * @return current builder
         */
        public Builder timestamp(Long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        /**
         * Sets record key.
         *
         * @param key message key
         * @return current builder
         */
        public Builder key(String key) {
            this.key = key;
            return this;
        }

        /**
         * Sets string headers for the record.
         *
         * @param headers message headers
         * @return current builder
         */
        public Builder headers(Map<String, String> headers) {
            this.headers = headers;
            return this;
        }

        /**
         * Overrides send timeout for this message.
         *
         * @param sendTimeout per-message send timeout
         * @return current builder
         */
        public Builder sendTimeout(Duration sendTimeout) {
            this.sendTimeout = sendTimeout;
            return this;
        }

        /**
         * Builds an immutable request.
         *
         * @return immutable send request
         */
        public KafkaMessageRequest build() {
            return new KafkaMessageRequest(this);
        }
    }

    private static <T> T requireNonNull(T value, String fieldName) {
        if (value == null) {
            throw new IllegalArgumentException(fieldName + " must not be null");
        }
        return value;
    }
}
