package com.example.kafka.common;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.SaslConfigs;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/** Loads connection settings from {@code .properties} (falling back to env vars) and builds base client config. */
public class KafkaConfig {

    private final Properties settings = new Properties();

    public KafkaConfig() {
        this("./.properties");
    }

    public KafkaConfig(String propertiesPath) {
        Path path = Path.of(propertiesPath);
        if (Files.exists(path)) {
            try (FileInputStream in = new FileInputStream(path.toFile())) {
                settings.load(in);
            } catch (IOException e) {
                throw new RuntimeException("Failed to load " + propertiesPath, e);
            }
        }
    }

    public String get(String key) {
        String value = settings.getProperty(key);
        if (value == null) {
            value = System.getenv(key);
        }
        if (value == null) {
            throw new IllegalStateException(
                    "Missing required setting '" + key + "'. Set it in .properties or as an environment variable.");
        }
        return value;
    }

    public String get(String key, String defaultValue) {
        String value = settings.getProperty(key);
        if (value == null) {
            value = System.getenv(key);
        }
        return value != null ? value : defaultValue;
    }

    public String topic() {
        return get("TOPIC", "orders-demo");
    }

    public String schemaRegistryUrl() {
        return get("SCHEMA_REGISTRY_URL");
    }

    /** Base client properties shared by producer and consumer: bootstrap server + Confluent Cloud SASL_SSL auth. */
    public Properties baseProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", get("BOOTSTRAP_SERVER"));
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
        props.put(SaslConfigs.SASL_JAAS_CONFIG, String.format(
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                get("API_KEY"), get("API_SECRET")));
        props.put("client.id", get("CLIENT_ID", "java-client"));
        return props;
    }

    /** Fails fast with a clear error if the cluster or topic isn't reachable. */
    public void verifyKafkaSetup() {
        Properties adminProps = new Properties();
        adminProps.putAll(baseProperties());
        adminProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);
        try (AdminClient admin = AdminClient.create(adminProps)) {
            Set<String> topics = admin.listTopics().names().get(10, TimeUnit.SECONDS);
            if (!topics.contains(topic())) {
                throw new IllegalStateException(
                        "Topic '" + topic() + "' does not exist on this cluster. Create it before running this demo.");
            }
        } catch (Exception e) {
            throw new RuntimeException("Could not verify Kafka connectivity: " + e.getMessage(), e);
        }
    }
}
