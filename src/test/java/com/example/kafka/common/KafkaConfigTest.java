package com.example.kafka.common;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaConfigTest {

    private Path tempProperties;

    @AfterEach
    void cleanup() throws IOException {
        if (tempProperties != null) {
            Files.deleteIfExists(tempProperties);
        }
    }

    @Test
    void baseProperties_setsSaslSslForConfluentCloud() throws IOException {
        tempProperties = Files.createTempFile("test", ".properties");
        Properties p = new Properties();
        p.setProperty("BOOTSTRAP_SERVER", "pkc-test.confluent.cloud:9092");
        p.setProperty("API_KEY", "test-key");
        p.setProperty("API_SECRET", "test-secret");
        try (var out = Files.newOutputStream(tempProperties)) {
            p.store(out, null);
        }

        KafkaConfig config = new KafkaConfig(tempProperties.toString());
        Properties props = config.baseProperties();

        assertEquals("SASL_SSL", props.get("security.protocol"));
        assertEquals("PLAIN", props.get("sasl.mechanism"));
        String jaas = (String) props.get("sasl.jaas.config");
        assertTrue(jaas.contains("test-key"));
        assertTrue(jaas.contains("test-secret"));
    }
}
