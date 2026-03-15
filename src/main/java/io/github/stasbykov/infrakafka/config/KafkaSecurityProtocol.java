package io.github.stasbykov.infrakafka.config;

import java.util.Locale;

/**
 * Supported values for Kafka {@code security.protocol}.
 */
public enum KafkaSecurityProtocol {
    PLAINTEXT,
    SASL_PLAINTEXT,
    SASL_SSL,
    SSL;

    /**
     * Parses an external configuration value into the enum constant.
     *
     * @param value raw external value
     * @return parsed protocol
     * @throws IllegalArgumentException when the value is null, blank or unsupported
     */
    public static KafkaSecurityProtocol fromExternalValue(String value) {
        if (isPresent(value)) {
            return KafkaSecurityProtocol.valueOf(normalized(value));
        }
        throw new IllegalArgumentException(String.format(Locale.ROOT, "Некорректное значение KafkaSecurityProtocol: %s", value));
    }

    private static boolean isPresent(String value) {
        return value != null && !value.isBlank();
    }

    private static String normalized(String str) {
        return str.trim().toUpperCase(Locale.ROOT);
    }
}
