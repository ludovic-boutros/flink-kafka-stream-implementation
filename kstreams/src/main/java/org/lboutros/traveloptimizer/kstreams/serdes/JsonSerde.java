package org.lboutros.traveloptimizer.kstreams.serdes;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.lboutros.traveloptimizer.kstreams.configuration.Constants;

import java.io.IOException;
import java.util.Map;

public abstract class JsonSerde<T> implements Serializer<T>, Deserializer<T>, Serde<T> {
    private static final ObjectMapper OBJECT_MAPPER = Constants.OBJECT_MAPPER;
    private TypeReference<T> typeReference;

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        typeReference = getTypeReference();
    }

    protected abstract TypeReference<T> getTypeReference();

    @Override
    public T deserialize(final String topic, final byte[] data) {
        if (data == null) {
            return null;
        }

        try {
            return OBJECT_MAPPER.readValue(data, typeReference);
        } catch (final IOException e) {
            throw new SerializationException(e);
        }
    }

    @Override
    public byte[] serialize(final String topic, final T data) {
        if (data == null) {
            return null;
        }

        try {
            return OBJECT_MAPPER.writeValueAsBytes(data);
        } catch (final Exception e) {
            throw new SerializationException("Error serializing JSON message", e);
        }
    }

    @Override
    public void close() {
    }

    @Override
    public Serializer<T> serializer() {
        return this;
    }

    @Override
    public Deserializer<T> deserializer() {
        return this;
    }

}
