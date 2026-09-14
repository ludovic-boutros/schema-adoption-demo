package com.example.kafka.producer;

import com.example.kafka.common.KafkaConfig;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * Registers the producer's JSON Schema against Schema Registry explicitly, at startup - never via
 * an auto-registering serializer. CI/CD registering schemas ahead of time (or, here, an explicit
 * call in main()) is what lets Schema Registry's compatibility check actually protect a topic.
 */
public final class SchemaRegistration {

    private static final Logger log = LoggerFactory.getLogger(SchemaRegistration.class);

    private SchemaRegistration() {
    }

    public static SchemaRegistryClient buildClient(KafkaConfig config) {
        List<SchemaProvider> providers = List.of(new JsonSchemaProvider());
        Map<String, Object> clientConfig = srAuthConfig(config);
        return new CachedSchemaRegistryClient(config.schemaRegistryUrl(), 100, providers, clientConfig);
    }

    private static Map<String, Object> srAuthConfig(KafkaConfig config) {
        String apiKey = config.get("SR_API_KEY", null);
        String apiSecret = config.get("SR_API_SECRET", null);
        if (apiKey == null || apiSecret == null) {
            return Map.of();
        }
        return Map.of(
                "basic.auth.credentials.source", "USER_INFO",
                "basic.auth.user.info", apiKey + ":" + apiSecret);
    }

    /** Reads a JSON Schema file and registers it under {@code subject}, returning its schema ID. */
    public static int registerSchema(SchemaRegistryClient client, String subject, String schemaResourcePath)
            throws IOException, RestClientException {
        String schemaString = Files.readString(Path.of(schemaResourcePath));
        JsonSchema schema = new JsonSchema(schemaString);
        return client.register(subject, schema);
    }

    /**
     * Sets {@code subject}'s compatibility level explicitly, overriding Confluent Cloud's
     * {@code BACKWARD} default for new subjects. This demo runs under {@code FORWARD}: the
     * producer owns the schema and is free to evolve it, and existing consumers - built against
     * an older version of the schema - must still be able to read whatever the producer sends
     * next. {@code BACKWARD} would instead protect a consumer that just upgraded to a newer
     * schema, which fits a team that owns the *reader* contract, not this demo's producer-owned
     * one.
     */
    public static void setCompatibility(SchemaRegistryClient client, String subject, String compatibility)
            throws IOException, RestClientException {
        client.updateCompatibility(subject, compatibility);
        log.info("Set compatibility for subject '{}' to {}", subject, compatibility);
    }

    /**
     * Deletes {@code subject} if it exists: a soft delete followed by a permanent one. The
     * permanent delete matters for a demo reset - a soft-deleted subject still counts toward
     * compatibility checks, so a fresh registration afterward could otherwise be rejected as
     * "incompatible" with a schema that's supposedly gone.
     */
    public static void deleteSubjectIfExists(SchemaRegistryClient client, String subject)
            throws IOException, RestClientException {
        if (!client.getAllSubjects().contains(subject)) {
            return;
        }
        client.deleteSubject(subject, false);
        client.deleteSubject(subject, true);
        log.info("Deleted schema subject '{}'", subject);
    }
}
