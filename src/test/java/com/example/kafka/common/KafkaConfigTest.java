package com.example.kafka.common;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

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

    @Test
    void ensureTopicsExist_onlyCreatesTopicsThatAreMissing() throws Exception {
        Admin admin = new MockAdminClient.Builder().numBrokers(1).build();
        admin.createTopics(List.of(new NewTopic("orders-demo", Optional.empty(), Optional.empty())))
                .all().get(10, TimeUnit.SECONDS);

        KafkaConfig.ensureTopicsExist(admin, List.of("orders-demo", "some-other-topic"));

        Set<String> topics = admin.listTopics().names().get(10, TimeUnit.SECONDS);
        assertTrue(topics.contains("orders-demo"));
        assertTrue(topics.contains("some-other-topic"));
    }
}
