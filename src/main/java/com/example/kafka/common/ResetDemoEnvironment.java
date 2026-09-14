package com.example.kafka.common;

import com.example.kafka.producer.SchemaRegistration;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Deletes this branch's demo topic(s) and schema subject if they exist, then recreates the topics
 * empty. Run this explicitly before a demo run to start from a clean slate - it is never called
 * automatically by the producer or consumer, since deleting a topic mid-demo would destroy
 * whatever the other side is currently reading or about to read.
 */
public class ResetDemoEnvironment {

    private static final Logger log = LoggerFactory.getLogger(ResetDemoEnvironment.class);

    public static void main(String[] args) throws Exception {
        KafkaConfig config = new KafkaConfig();
        String dlqTopic = config.topic() + "-dlq";
        String subject = config.topic() + "-value";

        log.info("Resetting demo environment for topics '{}'/'{}' and subject '{}'...",
                config.topic(), dlqTopic, subject);

        SchemaRegistryClient schemaRegistryClient = SchemaRegistration.buildClient(config);
        SchemaRegistration.deleteSubjectIfExists(schemaRegistryClient, subject);

        config.deleteTopicsIfExist(config.topic(), dlqTopic);
        config.ensureTopicsExist(config.topic(), dlqTopic);

        log.info("Done. '{}' and '{}' are empty, and '{}' has no registered schema.",
                config.topic(), dlqTopic, subject);
    }
}
