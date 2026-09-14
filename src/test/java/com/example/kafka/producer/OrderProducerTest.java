package com.example.kafka.producer;

import io.confluent.kafka.serializers.KafkaJsonSerializer;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class OrderProducerTest {

    @Test
    void send_recordsOneHistoryEntryWithOrderIdAsKey() {
        KafkaJsonSerializer<OrderEvent> valueSerializer = new KafkaJsonSerializer<>();
        valueSerializer.configure(Map.of(), false);
        MockProducer<String, OrderEvent> producer =
                new MockProducer<String, OrderEvent>(true, null, new StringSerializer(), valueSerializer);
        OrderEvent order = new OrderEvent("order-1", "customer-42", 19.99, "NEW");

        OrderProducer.send(producer, "orders-demo", order);

        assertEquals(1, producer.history().size());
        ProducerRecord<String, OrderEvent> sent = producer.history().get(0);
        assertEquals("order-1", sent.key());
        assertEquals(order, sent.value());
    }
}
