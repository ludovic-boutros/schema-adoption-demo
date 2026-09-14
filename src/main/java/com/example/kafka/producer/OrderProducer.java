package com.example.kafka.producer;

import com.example.kafka.common.KafkaConfig;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

/**
 * Sends plain JSON {@link OrderEvent} records — no schema, no Schema Registry. Uses Confluent's
 * {@link KafkaJsonSerializer}, which serializes a POJO to JSON bytes and never talks to Schema
 * Registry.
 */
public class OrderProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderProducer.class);

    public static void main(String[] args) {
        KafkaConfig config = new KafkaConfig();
        config.verifyKafkaSetup();

        Properties props = config.baseProperties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSerializer.class);

        KafkaProducer<String, OrderEvent> producer = new KafkaProducer<>(props);
        try {
            List<OrderEvent> orders = List.of(
                    new OrderEvent("order-1", "customer-42", 19.99, "NEW"),
                    new OrderEvent("order-2", "customer-17", 5.50, "NEW"),
                    new OrderEvent("order-3", "customer-42", 102.00, "PAID"));

            for (OrderEvent order : orders) {
                send(producer, config.topic(), order);
            }
        } finally {
            producer.flush();
            producer.close();
        }
    }

    static void send(Producer<String, OrderEvent> producer, String topic, OrderEvent order) {
        ProducerRecord<String, OrderEvent> record = new ProducerRecord<>(topic, order.getOrderId(), order);
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                log.error("Failed to send {}", order, exception);
            } else {
                log.info("Sent {} to partition {} offset {}", order, metadata.partition(), metadata.offset());
            }
        });
    }
}
