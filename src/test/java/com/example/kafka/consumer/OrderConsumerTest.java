package com.example.kafka.consumer;

import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;

class OrderConsumerTest {

    @Test
    void deserialize_readsMatchingJsonIntoOrderEvent() {
        KafkaJsonDeserializer<OrderEvent> deserializer = OrderConsumer.buildValueDeserializer();
        String json = "{\"orderId\":\"order-1\",\"customerId\":\"customer-42\",\"amount\":\"19.99\",\"status\":\"NEW\"}";

        OrderEvent event = deserializer.deserialize("orders-demo", json.getBytes(StandardCharsets.UTF_8));

        assertEquals("order-1", event.getOrderId());
        assertEquals("19.99", event.getAmount());
    }
}
