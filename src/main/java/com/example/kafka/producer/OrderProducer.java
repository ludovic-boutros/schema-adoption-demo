package com.example.kafka.producer;

import com.example.kafka.common.KafkaConfig;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializerConfig;
import io.confluent.kafka.serializers.schema.id.HeaderSchemaIdSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Sends {@link OrderEvent} records as plain JSON on the wire, exactly as before - but now the
 * schema behind that JSON is registered with Schema Registry, and the schema's GUID travels in
 * the {@code __value_schema_id} Kafka record header instead of the usual magic-byte prefix in the
 * payload - Confluent's documented "schema GUID in header" wire format
 * (see {@link #buildValueSerializer}). The consumer is completely unmodified by this change: same
 * payload bytes, a header it doesn't look at.
 *
 * <p>The subject's compatibility level is set to {@code FORWARD} before registering, since the
 * producer owns this data: it evolves the schema, and existing consumers must remain able to
 * read whatever it sends next.
 */
public class OrderProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderProducer.class);
    private static final String SCHEMA_PATH = "src/main/resources/schemas/order-event.schema.json";

    public static void main(String[] args) throws Exception {
        KafkaConfig config = new KafkaConfig();
        config.ensureTopicsExist(config.topic());

        String subject = config.topic() + "-value";
        SchemaRegistryClient schemaRegistryClient = SchemaRegistration.buildClient(config);
        SchemaRegistration.setCompatibility(schemaRegistryClient, subject, "FORWARD");
        int schemaId = SchemaRegistration.registerSchema(schemaRegistryClient, subject, SCHEMA_PATH);
        log.info("Registered schema for subject '{}' with id {}", subject, schemaId);

        Properties props = config.baseProperties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        KafkaProducer<String, OrderEvent> producer = new KafkaProducer<>(
                props, new StringSerializer(), buildValueSerializer(schemaRegistryClient, config.schemaRegistryUrl()));
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

    /**
     * Confluent's real JSON Schema serializer, configured to write the schema GUID to the
     * {@code __value_schema_id} record header ({@link HeaderSchemaIdSerializer}) instead of
     * prefixing it onto the payload ({@code PrefixSchemaIdSerializer}, the default) - this is what
     * keeps the JSON payload byte-for-byte identical to the no-schema producer. {@code
     * auto.register.schemas=false} + {@code use.latest.version=true} means this serializer only
     * looks up the schema {@link SchemaRegistration#registerSchema} already registered, never
     * registers on its own. {@code latest.compatibility.strict=false} skips a sanity check that
     * would otherwise compare that registered schema against one reflected from {@link
     * OrderEvent}'s fields - irrelevant here since the registered schema is the source of truth,
     * not the POJO shape. {@code schema.registry.url} is required by {@code
     * KafkaJsonSchemaSerializerConfig} even though {@code client} is already built and connected -
     * it's only read if no client were supplied.
     */
    static KafkaJsonSchemaSerializer<OrderEvent> buildValueSerializer(SchemaRegistryClient client, String schemaRegistryUrl) {
        return new KafkaJsonSchemaSerializer<>(client, Map.of(
                KafkaJsonSchemaSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl,
                KafkaJsonSchemaSerializerConfig.AUTO_REGISTER_SCHEMAS, false,
                KafkaJsonSchemaSerializerConfig.USE_LATEST_VERSION, true,
                KafkaJsonSchemaSerializerConfig.LATEST_COMPATIBILITY_STRICT, false,
                KafkaJsonSchemaSerializerConfig.VALUE_SCHEMA_ID_SERIALIZER, HeaderSchemaIdSerializer.class));
    }
}
