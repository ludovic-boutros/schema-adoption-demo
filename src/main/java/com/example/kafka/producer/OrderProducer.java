package com.example.kafka.producer;

import com.example.kafka.common.KafkaConfig;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
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
 * Sends {@link OrderEvent} records as plain JSON on the wire, exactly as before - but now the
 * schema behind that JSON is registered with Schema Registry, and the schema ID travels in a
 * Kafka record header via {@link HeaderEncodedJsonSerializer}. The consumer is completely
 * unmodified by this change: same payload bytes, headers it doesn't look at.
 */
public class OrderProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderProducer.class);
    private static final String SCHEMA_PATH = "src/main/resources/schemas/order-event.schema.json";

    public static void main(String[] args) throws Exception {
        KafkaConfig config = new KafkaConfig();
        config.ensureTopicsExist(config.topic());

        String subject = config.topic() + "-value";
        SchemaRegistryClient schemaRegistryClient = SchemaRegistration.buildClient(config);
        int schemaId = SchemaRegistration.registerSchema(schemaRegistryClient, subject, SCHEMA_PATH);
        log.info("Registered schema for subject '{}' with id {}", subject, schemaId);

        Properties props = config.baseProperties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        KafkaProducer<String, OrderEvent> producer = new KafkaProducer<>(
                props, new StringSerializer(), new HeaderEncodedJsonSerializer<>(schemaId, subject));
        try {
            List<OrderEvent> orders = List.of(
                    new OrderEvent("order-1", "customer-42", "19.99", "NEW"),
                    new OrderEvent("order-2", "customer-17", "5.50", "NEW"),
                    new OrderEvent("order-3", "customer-42", "102.00", "PAID"));

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
