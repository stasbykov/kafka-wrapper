package io.github.stasbykov.infrakafka.consumer;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConsumerWrapperTest {

    @Test
    void groupedOperationsShouldFailFastWhenGroupIdIsMissing() {
        ConsumerWrapper consumerWrapper = new ConsumerWrapper(new Properties(), Duration.ofMillis(100), Duration.ofSeconds(1));

        assertGroupError(() -> consumerWrapper.subscribe(List.of("topic")));
        assertGroupError(() -> consumerWrapper.pollManaged(Duration.ofMillis(100)));
        assertGroupError(consumerWrapper::commit);
        assertGroupError(() -> consumerWrapper.seek("topic", 0, 0));
        assertGroupError(() -> consumerWrapper.seekToBeginning("topic"));
        assertGroupError(() -> consumerWrapper.seekToEnd("topic"));
        assertGroupError(() -> consumerWrapper.getCommittedOffsets("topic"));
    }

    private static void assertGroupError(Runnable action) {
        IllegalStateException exception = assertThrows(IllegalStateException.class, action::run);
        assertTrue(exception.getMessage().contains("groupId is required"));
    }
}
