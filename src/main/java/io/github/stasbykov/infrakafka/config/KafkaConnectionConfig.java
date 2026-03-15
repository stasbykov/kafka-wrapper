package io.github.stasbykov.infrakafka.config;

import org.aeonbits.owner.Config;

/**
 * OWNER-based configuration contract for loading Kafka connection settings
 * from system properties or environment variables.
 */
@Config.Sources({
        "system:env",
        "system:properties"
})
@Config.LoadPolicy(Config.LoadType.MERGE)
public interface KafkaConnectionConfig extends Config {

    @Key("${service}.servers")
    String servers();

    @Key("${service}.keystore")
    String keyStore();

    @Key("${service}.keystorepass")
    String keyStorePass();

    @Key("${service}.truststore")
    String trustStore();

    @Key("${service}.truststorepass")
    String trustStorePass();

    @Key("${service}.security.protocol")
    @DefaultValue("PLAINTEXT")
    String securityProtocol();
}
