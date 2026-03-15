package io.github.stasbykov.infrakafka.serializer;

/**
 * String serializer adapter for Kafka client configuration.
 */
public class StringSerializer implements Serializer {

    @Override
    public String serializerClassName() {
        return org.apache.kafka.common.serialization.StringSerializer.class.getName();
    }
}
