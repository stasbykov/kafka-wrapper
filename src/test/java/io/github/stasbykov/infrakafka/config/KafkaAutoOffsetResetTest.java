package io.github.stasbykov.infrakafka.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class KafkaAutoOffsetResetTest {

    @Test
    void fromExternalValueShouldHandleCaseAndSpaces() {
        assertEquals(KafkaAutoOffsetReset.EARLIEST, KafkaAutoOffsetReset.fromExternalValue(" earliest "));
        assertEquals(KafkaAutoOffsetReset.LATEST, KafkaAutoOffsetReset.fromExternalValue("LATEST"));
        assertEquals(KafkaAutoOffsetReset.NONE, KafkaAutoOffsetReset.fromExternalValue("none"));
    }

    @Test
    void fromExternalValueShouldRejectInvalidValues() {
        assertThrows(IllegalArgumentException.class, () -> KafkaAutoOffsetReset.fromExternalValue("earli"));
        assertThrows(IllegalArgumentException.class, () -> KafkaAutoOffsetReset.fromExternalValue(" "));
        assertThrows(IllegalArgumentException.class, () -> KafkaAutoOffsetReset.fromExternalValue(null));
    }

    @Test
    void externalValueShouldReturnLowercaseKafkaValue() {
        assertEquals("earliest", KafkaAutoOffsetReset.EARLIEST.externalValue());
        assertEquals("latest", KafkaAutoOffsetReset.LATEST.externalValue());
        assertEquals("none", KafkaAutoOffsetReset.NONE.externalValue());
    }
}
