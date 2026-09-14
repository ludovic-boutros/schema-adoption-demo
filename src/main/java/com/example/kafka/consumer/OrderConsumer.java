package com.example.kafka.consumer;

import com.example.kafka.common.KafkaConfig;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.cfg.CoercionAction;
import com.fasterxml.jackson.databind.cfg.CoercionInputShape;
import com.fasterxml.jackson.databind.type.LogicalType;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.KafkaJsonDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Reads plain JSON {@link OrderEvent} records — no schema, no Schema Registry. Uses Confluent's
 * {@link KafkaJsonDeserializer}, which deserializes JSON bytes straight into a POJO and never
 * talks to Schema Registry.
 */
public class OrderConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderConsumer.class);

    public static void main(String[] args) {
        KafkaConfig config = new KafkaConfig();
        config.verifyKafkaSetup();

        Properties props = config.baseProperties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, config.get("GROUP_ID", "orders-demo-consumer"));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        KafkaJsonDeserializer<OrderEvent> valueDeserializer = buildValueDeserializer();

        KafkaConsumer<String, OrderEvent> consumer =
                new KafkaConsumer<>(props, new StringDeserializer(), valueDeserializer);

        Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down consumer...");
            consumer.wakeup();
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));

        try {
            consumer.subscribe(List.of(config.topic()));
            while (true) {
                ConsumerRecords<String, OrderEvent> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, OrderEvent> record : records) {
                    log.info("Received {}", record.value());
                }
                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            }
        } catch (WakeupException e) {
            log.info("Consumer loop interrupted for shutdown");
        } finally {
            consumer.close();
        }
    }

    /**
     * Scalar coercion (e.g. JSON string "42.50" silently becoming a double) is disabled: Jackson's
     * default leniency would otherwise mask type-drift bugs instead of surfacing them, which
     * defeats the point of this demo.
     */
    static KafkaJsonDeserializer<OrderEvent> buildValueDeserializer() {
        KafkaJsonDeserializer<OrderEvent> deserializer = new KafkaJsonDeserializer<>();
        deserializer.configure(Map.of(KafkaJsonDeserializerConfig.JSON_VALUE_TYPE, OrderEvent.class), false);
        deserializer.objectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        deserializer.objectMapper().coercionConfigFor(LogicalType.Integer)
                .setCoercion(CoercionInputShape.String, CoercionAction.Fail);
        deserializer.objectMapper().coercionConfigFor(LogicalType.Float)
                .setCoercion(CoercionInputShape.String, CoercionAction.Fail);
        deserializer.objectMapper().coercionConfigFor(LogicalType.Boolean)
                .setCoercion(CoercionInputShape.String, CoercionAction.Fail);
        return deserializer;
    }
}
