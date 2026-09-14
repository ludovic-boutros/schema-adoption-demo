package com.example.kafka.producer;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderProducerRejectionTest {

    private static final String SCHEMA_PATH = "src/main/resources/schemas/order-event.schema.json";

    @Mock
    private SchemaRegistryClient client;

    @Test
    void registerOrReject_returnsEmptyWhenSchemaRegistryRejectsTheChange() throws Exception {
        when(client.register(eq("orders-demo-value"), any(ParsedSchema.class)))
                .thenThrow(new RestClientException("Schema being registered is incompatible with an earlier schema",
                        409, 409));

        Optional<Integer> result = OrderProducer.registerOrReject(client, "orders-demo-value", SCHEMA_PATH);

        assertTrue(result.isEmpty());
    }

    @Test
    void registerOrReject_returnsIdWhenSchemaRegistryAccepts() throws Exception {
        when(client.register(eq("orders-demo-value"), any(ParsedSchema.class))).thenReturn(9);

        Optional<Integer> result = OrderProducer.registerOrReject(client, "orders-demo-value", SCHEMA_PATH);

        assertEquals(Optional.of(9), result);
    }
}
