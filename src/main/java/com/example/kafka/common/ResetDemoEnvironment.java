package com.example.kafka.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Deletes this branch's demo topic(s) if they exist, then recreates them empty. Run this
 * explicitly before a demo run to start from a clean slate - it is never called automatically by
 * the producer or consumer, since deleting a topic mid-demo would destroy whatever the other side
 * is currently reading or about to read.
 */
public class ResetDemoEnvironment {

    private static final Logger log = LoggerFactory.getLogger(ResetDemoEnvironment.class);

    public static void main(String[] args) {
        KafkaConfig config = new KafkaConfig();
        String dlqTopic = config.topic() + "-dlq";

        log.info("Resetting demo environment for topics '{}' and '{}'...", config.topic(), dlqTopic);
        config.deleteTopicsIfExist(config.topic(), dlqTopic);
        config.ensureTopicsExist(config.topic(), dlqTopic);
        log.info("Done. '{}' and '{}' are empty and ready.", config.topic(), dlqTopic);
    }
}
