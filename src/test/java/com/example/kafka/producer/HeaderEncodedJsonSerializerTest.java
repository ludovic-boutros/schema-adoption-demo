package com.example.kafka.producer;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class HeaderEncodedJsonSerializerTest {

    @Test
    void serialize_writesSchemaIdAndSubjectToHeadersButKeepsPlainJsonPayload() {
        HeaderEncodedJsonSerializer<OrderEvent> serializer = new HeaderEncodedJsonSerializer<>(7, "orders-demo-value");
        OrderEvent order = new OrderEvent("order-1", "customer-42", "19.99", "NEW");
        RecordHeaders headers = new RecordHeaders();

        byte[] payload = serializer.serialize("orders-demo", headers, order);

        assertEquals(
                "{\"orderId\":\"order-1\",\"customerId\":\"customer-42\",\"amount\":\"19.99\","
                        + "\"status\":\"NEW\"}",
                new String(payload, StandardCharsets.UTF_8));

        Header schemaIdHeader = headers.lastHeader(HeaderEncodedJsonSerializer.SCHEMA_ID_HEADER);
        assertNotNull(schemaIdHeader);
        assertEquals(7, ByteBuffer.wrap(schemaIdHeader.value()).getInt());

        Header subjectHeader = headers.lastHeader(HeaderEncodedJsonSerializer.SCHEMA_SUBJECT_HEADER);
        assertNotNull(subjectHeader);
        assertEquals("orders-demo-value", new String(subjectHeader.value(), StandardCharsets.UTF_8));
    }
}
