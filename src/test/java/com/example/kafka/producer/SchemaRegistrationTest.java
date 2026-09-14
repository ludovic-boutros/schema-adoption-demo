package com.example.kafka.producer;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SchemaRegistrationTest {

    private static final String SCHEMA_PATH = "src/main/resources/schemas/order-event.schema.json";

    @Mock
    private SchemaRegistryClient client;

    @Test
    void registerSchema_returnsIdFromRegistryClient() throws Exception {
        when(client.register(eq("orders-demo-value"), any(ParsedSchema.class))).thenReturn(7);

        int schemaId = SchemaRegistration.registerSchema(client, "orders-demo-value", SCHEMA_PATH);

        assertEquals(7, schemaId);
    }

    @Test
    void registerSchema_propagatesIncompatibleSchemaRejection() throws Exception {
        RestClientException rejection =
                new RestClientException("Schema being registered is incompatible with an earlier schema", 409, 409);
        when(client.register(eq("orders-demo-value"), any(ParsedSchema.class))).thenThrow(rejection);

        RestClientException thrown = assertThrows(RestClientException.class,
                () -> SchemaRegistration.registerSchema(client, "orders-demo-value", SCHEMA_PATH));
        assertEquals(409, thrown.getErrorCode());
    }

    @Test
    void deleteSubjectIfExists_softAndHardDeletesWhenSubjectExists() throws Exception {
        when(client.getAllSubjects()).thenReturn(Set.of("orders-demo-value"));

        SchemaRegistration.deleteSubjectIfExists(client, "orders-demo-value");

        verify(client).deleteSubject("orders-demo-value", false);
        verify(client).deleteSubject("orders-demo-value", true);
    }

    @Test
    void deleteSubjectIfExists_doesNothingWhenSubjectIsAbsent() throws Exception {
        when(client.getAllSubjects()).thenReturn(Set.of());

        SchemaRegistration.deleteSubjectIfExists(client, "orders-demo-value");

        verify(client, never()).deleteSubject(anyString(), anyBoolean());
    }
}
