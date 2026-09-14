package com.example.kafka.producer;

import io.confluent.kafka.serializers.KafkaJsonSerializer;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Wraps Confluent's plain {@link KafkaJsonSerializer} to additionally record which schema this
 * message was produced against - as a Kafka record header, not the usual Confluent wire-format
 * prefix (magic byte + schema ID) in the payload.
 *
 * This keeps the payload bytes byte-for-byte identical to the no-schema producer: a consumer that
 * doesn't know about the header (i.e. every consumer written before this change) reads the message
 * exactly as before. The header is purely additive information for anyone who wants to look up the
 * exact schema a message was validated against.
 */
public class HeaderEncodedJsonSerializer<T> implements Serializer<T> {

    public static final String SCHEMA_ID_HEADER = "schema-id";
    public static final String SCHEMA_SUBJECT_HEADER = "schema-subject";

    private final KafkaJsonSerializer<T> delegate = new KafkaJsonSerializer<>();
    private final int schemaId;
    private final String subject;

    public HeaderEncodedJsonSerializer(int schemaId, String subject) {
        this.schemaId = schemaId;
        this.subject = subject;
        delegate.configure(Map.of(), false);
    }

    @Override
    public byte[] serialize(String topic, T data) {
        return serialize(topic, null, data);
    }

    @Override
    public byte[] serialize(String topic, Headers headers, T data) {
        if (headers != null) {
            headers.add(SCHEMA_ID_HEADER, ByteBuffer.allocate(4).putInt(schemaId).array());
            headers.add(SCHEMA_SUBJECT_HEADER, subject.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }
        return delegate.serialize(topic, data);
    }
}
