package com.example.kafka.consumer;

import com.example.kafka.common.KafkaConfig;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.cfg.CoercionAction;
import com.fasterxml.jackson.databind.cfg.CoercionInputShape;
import com.fasterxml.jackson.databind.type.LogicalType;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.KafkaJsonDeserializerConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Reads plain JSON {@link OrderEvent} records — no schema, no Schema Registry. Uses Confluent's
 * {@link KafkaJsonDeserializer}, which deserializes JSON bytes straight into a POJO and never
 * talks to Schema Registry.
 *
 * A record this consumer can't deserialize (a "poison pill") would otherwise block its offset
 * forever - {@code poll()} throws again on every retry, so the position never advances. Instead of
 * crashing, this consumer publishes the untouched raw bytes to a dead-letter topic
 * ({@code <topic>-dlq}) and seeks past the bad offset, so it keeps running.
 */
public class OrderConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderConsumer.class);

    public static void main(String[] args) {
        KafkaConfig config = new KafkaConfig();
        String dlqTopic = config.topic() + "-dlq";
        config.ensureTopicsExist(config.topic(), dlqTopic);

        Properties props = config.baseProperties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, config.get("GROUP_ID", "orders-demo-consumer"));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        KafkaJsonDeserializer<OrderEvent> valueDeserializer = buildValueDeserializer();

        KafkaConsumer<String, OrderEvent> consumer =
                new KafkaConsumer<>(props, new StringDeserializer(), valueDeserializer);
        Producer<byte[], byte[]> dlqProducer = buildDlqProducer(config);

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
                ConsumerRecords<String, OrderEvent> records;
                try {
                    records = consumer.poll(Duration.ofMillis(1000));
                } catch (RecordDeserializationException e) {
                    handlePoisonPill(dlqProducer, dlqTopic, consumer, e);
                    continue;
                }
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
            dlqProducer.close();
        }
    }

    /**
     * Publishes the untouched raw key/value/headers of a record that failed to deserialize to the
     * DLQ topic, then seeks the consumer past it. {@link RecordDeserializationException} (KIP-334)
     * carries exactly the raw bytes and position needed to do this without re-fetching anything.
     */
    static void handlePoisonPill(Producer<byte[], byte[]> dlqProducer, String dlqTopic,
            Consumer<?, ?> consumer, RecordDeserializationException e) {
        Throwable cause = e.getCause() != null ? e.getCause() : e;
        log.error("Poison pill on {}-{} at offset {}: {}", e.topicPartition().topic(),
                e.topicPartition().partition(), e.offset(), cause.getMessage());

        byte[] key = e.keyBuffer() != null ? toBytes(e.keyBuffer()) : null;
        byte[] value = e.valueBuffer() != null ? toBytes(e.valueBuffer()) : null;
        ProducerRecord<byte[], byte[]> dlqRecord = new ProducerRecord<>(dlqTopic, null, key, value);
        for (Header header : e.headers()) {
            dlqRecord.headers().add(header);
        }

        dlqProducer.send(dlqRecord, (metadata, sendException) -> {
            if (sendException != null) {
                log.error("Failed to publish poison pill to DLQ topic '{}'", dlqTopic, sendException);
            } else {
                log.info("Published poison pill to DLQ topic '{}' at offset {}", dlqTopic, metadata.offset());
            }
        });
        dlqProducer.flush();

        consumer.seek(e.topicPartition(), e.offset() + 1);
        log.warn("Skipped past offset {} on {} - the consumer keeps running instead of crashing.",
                e.offset(), e.topicPartition());
    }

    private static byte[] toBytes(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.duplicate().get(bytes);
        return bytes;
    }

    private static Producer<byte[], byte[]> buildDlqProducer(KafkaConfig config) {
        Properties props = config.baseProperties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, config.get("CLIENT_ID", "java-client") + "-dlq-producer");
        return new KafkaProducer<>(props);
    }

    /**
     * Scalar coercion (e.g. JSON number 19.99 silently becoming the string "19.99") is disabled:
     * Jackson's default leniency would otherwise mask type-drift bugs instead of surfacing them,
     * which defeats the point of this demo.
     */
    static KafkaJsonDeserializer<OrderEvent> buildValueDeserializer() {
        KafkaJsonDeserializer<OrderEvent> deserializer = new KafkaJsonDeserializer<>();
        deserializer.configure(Map.of(KafkaJsonDeserializerConfig.JSON_VALUE_TYPE, OrderEvent.class), false);
        deserializer.objectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        deserializer.objectMapper().coercionConfigFor(LogicalType.Textual)
                .setCoercion(CoercionInputShape.Integer, CoercionAction.Fail);
        deserializer.objectMapper().coercionConfigFor(LogicalType.Textual)
                .setCoercion(CoercionInputShape.Float, CoercionAction.Fail);
        deserializer.objectMapper().coercionConfigFor(LogicalType.Integer)
                .setCoercion(CoercionInputShape.String, CoercionAction.Fail);
        deserializer.objectMapper().coercionConfigFor(LogicalType.Float)
                .setCoercion(CoercionInputShape.String, CoercionAction.Fail);
        deserializer.objectMapper().coercionConfigFor(LogicalType.Boolean)
                .setCoercion(CoercionInputShape.String, CoercionAction.Fail);
        return deserializer;
    }
}
