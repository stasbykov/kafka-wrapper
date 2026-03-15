package io.github.stasbykov.infrakafka.deserializer;

/**
 * Abstraction for providing Kafka deserializer class names to the builder API.
 */
public interface Deserializer {

    /**
     * @return fully qualified Kafka deserializer class name
     */
    String deserializerClassName();
}
