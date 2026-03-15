package io.github.stasbykov.infrakafka.deserializer;

/**
 * String deserializer adapter for Kafka client configuration.
 */
public class StringDeserializer implements Deserializer {

    @Override
    public String deserializerClassName() {
        return org.apache.kafka.common.serialization.StringDeserializer.class.getName();
    }
}
