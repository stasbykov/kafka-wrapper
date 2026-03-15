package io.github.stasbykov.infrakafka.serializer;

/**
 * Abstraction for providing Kafka serializer class names to the builder API.
 */
public interface Serializer {

    /**
     * @return fully qualified Kafka serializer class name
     */
    String serializerClassName();
}
