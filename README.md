# kafka-wrapper

Small Java library for string-based Kafka producer/consumer workflows in tests and service utilities.

## What it provides

- `KafkaClient` facade for send, batch send, polling, waiting and offset inspection
- config bootstrap from system properties and environment variables via OWNER
- immutable message/result models
- isolated consumer group generation for test scenarios

## Quick start

```java
try (KafkaClient client = KafkaClient.builder()
        .bootstrapServers("localhost:9092")
        .useIsolatedGroupId("smoke")
        .build()) {
    client.sendMessage("events", "key-1", "payload");
    String value = client.getMessage("events", "key-1");
}
```

## Configuration

`KafkaClient.builder().fromConfig("qa.kafka")` reads:

- `qa.kafka.servers`
- `qa.kafka.keystore`
- `qa.kafka.keystorepass`
- `qa.kafka.truststore`
- `qa.kafka.truststorepass`
- `qa.kafka.security.protocol`

## Notes

- `poll(...)` reads only new messages from the current tail of the topic.
- `readFromBeginning(...)` is the explicit API for historical reads.
- managed consumer operations are serialized; do not expect parallel polling from one `KafkaClient` instance.

## Build

```bash
./gradlew test
./gradlew publishToMavenLocal
```
