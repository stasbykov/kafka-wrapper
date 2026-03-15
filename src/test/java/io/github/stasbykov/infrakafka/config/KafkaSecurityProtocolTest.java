package io.github.stasbykov.infrakafka.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class KafkaSecurityProtocolTest {

    @Test
    void fromExternalValueShouldHandleCaseAndSpaces() {
        assertEquals(KafkaSecurityProtocol.SASL_SSL, KafkaSecurityProtocol.fromExternalValue(" sasl_ssl "));
        assertEquals(KafkaSecurityProtocol.PLAINTEXT, KafkaSecurityProtocol.fromExternalValue("plaintext"));
    }

    @Test
    void fromExternalValueShouldRejectBlankValue() {
        assertThrows(IllegalArgumentException.class, () -> KafkaSecurityProtocol.fromExternalValue(" "));
        assertThrows(IllegalArgumentException.class, () -> KafkaSecurityProtocol.fromExternalValue(null));
    }
}
