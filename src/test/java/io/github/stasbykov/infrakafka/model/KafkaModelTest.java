package io.github.stasbykov.infrakafka.model;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class KafkaModelTest {

    @Test
    void kafkaMessageRequestShouldPreserveValuesAndProtectHeadersFromMutation() {
        Map<String, String> headers = new HashMap<>();
        headers.put("traceId", "123");

        KafkaMessageRequest request = KafkaMessageRequest.builder("payload")
                .key("key-1")
                .partition(2)
                .timestamp(42L)
                .headers(headers)
                .sendTimeout(Duration.ofSeconds(5))
                .build();

        headers.put("traceId", "456");

        assertEquals("payload", request.value());
        assertEquals("key-1", request.key());
        assertEquals(2, request.partition());
        assertEquals(42L, request.timestamp());
        assertEquals(Duration.ofSeconds(5), request.sendTimeout());
        assertEquals("123", request.headers().get("traceId"));
        assertThrows(UnsupportedOperationException.class, () -> request.headers().put("new", "value"));
    }

    @Test
    void kafkaMessageRequestShouldRejectNullValue() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> KafkaMessageRequest.builder(null).build()
        );

        assertTrue(exception.getMessage().contains("value"));
    }

    @Test
    void kafkaSendResultShouldExposeAssignedValues() {
        KafkaSendResult result = new KafkaSendResult("audit", 1, 10L, 123456789L);

        assertEquals("audit", result.topic());
        assertEquals(1, result.partition());
        assertEquals(10L, result.offset());
        assertEquals(123456789L, result.timestamp());
    }
}
