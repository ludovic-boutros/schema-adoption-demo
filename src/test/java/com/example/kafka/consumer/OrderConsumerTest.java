package com.example.kafka.consumer;

import com.example.kafka.producer.OrderEvent;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class OrderConsumerTest {

    @Test
    void deserialize_readsMatchingJsonIntoOrderEvent() {
        KafkaJsonDeserializer<com.example.kafka.consumer.OrderEvent> deserializer = OrderConsumer.buildValueDeserializer();
        String json = "{\"orderId\":\"order-1\",\"customerId\":\"customer-42\",\"amount\":\"19.99\",\"status\":\"NEW\"}";

        com.example.kafka.consumer.OrderEvent event =
                deserializer.deserialize("orders-demo", json.getBytes(StandardCharsets.UTF_8));

        assertEquals("order-1", event.getOrderId());
        assertEquals("19.99", event.getAmount());
    }

    @Test
    void deserialize_breaksOnTheProducersNewNumericAmountFormat() {
        // The producer's current OrderEvent - unrelated Java class, but same field names - now
        // sends amount as a Double instead of a String.
        KafkaJsonSerializer<OrderEvent> producerSideSerializer = new KafkaJsonSerializer<>();
        producerSideSerializer.configure(Map.of(), false);
        OrderEvent producedOrder = new OrderEvent("order-1", "customer-42", 19.99, "NEW");
        byte[] bytesOnTheWire = producerSideSerializer.serialize("orders-demo", producedOrder);

        // The consumer never changed: it still expects amount to be a String.
        KafkaJsonDeserializer<com.example.kafka.consumer.OrderEvent> deserializer = OrderConsumer.buildValueDeserializer();

        assertThrows(SerializationException.class, () -> deserializer.deserialize("orders-demo", bytesOnTheWire));
    }
}
