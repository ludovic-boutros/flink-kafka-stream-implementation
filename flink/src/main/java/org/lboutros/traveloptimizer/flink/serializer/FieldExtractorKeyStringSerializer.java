package org.lboutros.traveloptimizer.flink.serializer;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public abstract class FieldExtractorKeyStringSerializer<T> implements Serializer<T> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    public abstract String extractKey(T t);

    @Override
    public byte[] serialize(String topic, T data) {
        return data == null ? null : extractKey(data).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] serialize(String topic, Headers headers, T data) {
        return data == null ? null : extractKey(data).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void close() {
    }
}
