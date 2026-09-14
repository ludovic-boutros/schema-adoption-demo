package com.example.kafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class OrderConsumerPoisonPillTest {

    @Mock
    private Consumer<?, ?> consumer;

    @Test
    void handlePoisonPill_publishesRawBytesToDlqAndSeeksPastTheOffset() {
        TopicPartition topicPartition = new TopicPartition("orders-demo", 0);
        byte[] keyBytes = "order-1".getBytes(StandardCharsets.UTF_8);
        byte[] valueBytes = "{\"orderId\":\"order-1\",\"amount\":\"$19.99\"}".getBytes(StandardCharsets.UTF_8);
        RecordHeaders headers = new RecordHeaders();
        headers.add(new RecordHeader("trace-id", "abc-123".getBytes(StandardCharsets.UTF_8)));

        RecordDeserializationException exception = new RecordDeserializationException(
                RecordDeserializationException.DeserializationExceptionOrigin.VALUE,
                topicPartition, 42L, System.currentTimeMillis(), TimestampType.CREATE_TIME,
                ByteBuffer.wrap(keyBytes), ByteBuffer.wrap(valueBytes), headers,
                "Cannot deserialize value", new RuntimeException("Cannot deserialize value"));

        MockProducer<byte[], byte[]> dlqProducer =
                new MockProducer<byte[], byte[]>(true, null, new ByteArraySerializer(), new ByteArraySerializer());

        OrderConsumer.handlePoisonPill(dlqProducer, "orders-demo-dlq", consumer, exception);

        assertEquals(1, dlqProducer.history().size());
        ProducerRecord<byte[], byte[]> dlqRecord = dlqProducer.history().get(0);
        assertEquals("orders-demo-dlq", dlqRecord.topic());
        assertArrayEquals(keyBytes, dlqRecord.key());
        assertArrayEquals(valueBytes, dlqRecord.value());
        assertArrayEquals("abc-123".getBytes(StandardCharsets.UTF_8), dlqRecord.headers().lastHeader("trace-id").value());

        verify(consumer).seek(topicPartition, 43L);
    }
}
