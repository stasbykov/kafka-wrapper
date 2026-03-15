package io.github.stasbykov.infrakafka.config;

import java.util.Locale;

/**
 * Supported values for Kafka {@code auto.offset.reset}.
 */
public enum KafkaAutoOffsetReset {
    EARLIEST,
    LATEST,
    NONE;

    /**
     * Returns the lowercase value expected by Kafka configuration.
     *
     * @return external Kafka value
     */
    public String externalValue() {
        return name().toLowerCase(Locale.ROOT);
    }

    /**
     * Parses an external configuration value into the enum constant.
     *
     * @param value raw external value
     * @return parsed offset reset mode
     * @throws IllegalArgumentException when the value is null, blank or unsupported
     */
    public static KafkaAutoOffsetReset fromExternalValue(String value) {
        if (isPresent(value)) {
            return KafkaAutoOffsetReset.valueOf(normalized(value));
        }
        throw new IllegalArgumentException(String.format(Locale.ROOT, "Некорректное значение KafkaAutoOffsetReset: %s", value));
    }

    private static boolean isPresent(String value) {
        return value != null && !value.isBlank();
    }

    private static String normalized(String value) {
        return value.trim().toUpperCase(Locale.ROOT);
    }
}
