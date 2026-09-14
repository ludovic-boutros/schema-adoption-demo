package com.example.kafka.common;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.SaslConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/** Loads connection settings from {@code .properties} (falling back to env vars) and builds base client config. */
public class KafkaConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConfig.class);

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

    /**
     * Creates any of the given topics that don't already exist, so running this demo never
     * requires a manual "create the topic first" step in the Confluent Cloud console.
     *
     * The service account behind API_KEY needs the ResourceOwner role on these topics (or
     * broader) for topic creation to succeed - see the README.
     */
    public void ensureTopicsExist(String... topics) {
        Properties adminProps = new Properties();
        adminProps.putAll(baseProperties());
        adminProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);
        try (Admin admin = AdminClient.create(adminProps)) {
            ensureTopicsExist(admin, Arrays.asList(topics));
        } catch (Exception e) {
            throw new RuntimeException("Could not ensure topics exist: " + e.getMessage(), e);
        }
    }

    /** Split out from {@link #ensureTopicsExist(String...)} so it's testable against a mock {@link Admin}. */
    static void ensureTopicsExist(Admin admin, List<String> topics) throws Exception {
        Set<String> existing = admin.listTopics().names().get(10, TimeUnit.SECONDS);
        List<NewTopic> missing = topics.stream()
                .filter(t -> !existing.contains(t))
                .map(t -> new NewTopic(t, Optional.empty(), Optional.empty()))
                .collect(Collectors.toList());
        if (missing.isEmpty()) {
            return;
        }
        admin.createTopics(missing).all().get(30, TimeUnit.SECONDS);
        missing.forEach(t -> log.info("Created topic '{}'", t.name()));
    }
}
