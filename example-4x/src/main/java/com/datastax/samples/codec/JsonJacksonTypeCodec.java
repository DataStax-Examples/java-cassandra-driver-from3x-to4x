package com.datastax.samples.codec;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;

/**
 * Convert some PJP into JSON.
 *
 * @param <T>
 */
public class JsonJacksonTypeCodec<T extends Serializable> implements TypeCodec<T> {

    /**
     * Jackson will help us here json <-> POJO
     */
    private final ObjectMapper objectMapper = new ObjectMapper();

    private Class<T> javaType;
    
    public JsonJacksonTypeCodec(Class<T> classType) {
        this.javaType = classType;
    }
    
    /** {@inheritDoc} */
    @Override
    public GenericType<T> getJavaType() {
        return GenericType.of(javaType);
    }

    /** {@inheritDoc} */
    @Override
    public DataType getCqlType() {
        return DataTypes.TEXT;
    }

    /** {@inheritDoc} */
    @Override
    public ByteBuffer encode(T value, ProtocolVersion protocolVersion) {
        if (value == null)
            return null;
        try {
            return ByteBuffer.wrap(objectMapper.writeValueAsBytes(value));
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    /** {@inheritDoc} */
    @Override
    @SuppressWarnings("unchecked")
    public T decode(ByteBuffer bytes, ProtocolVersion protocolVersion) {
        if (bytes == null)
            return null;
        try {
            byte[] b = new byte[bytes.remaining()];
            bytes.duplicate().get(b);
            return (T) objectMapper.readValue(b, TypeFactory.defaultInstance().constructType(getJavaType().getType()));
        } catch (IOException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public String format(T value) {
        if (value == null)
            return "NULL";
        String json;
        try {
            json = objectMapper.writeValueAsString(value);
        } catch (IOException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
        return '\'' + json.replace("\'", "''") + '\'';
    }

    /** {@inheritDoc} */
    @Override
    @SuppressWarnings("unchecked")
    public T parse(String value) {
        if (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL"))
            return null;
        if (value.charAt(0) != '\'' || value.charAt(value.length() - 1) != '\'')
            throw new IllegalArgumentException("JSON strings must be enclosed by single quotes");
        String json = value.substring(1, value.length() - 1).replace("''", "'");
        try {
            return (T) objectMapper.readValue(json, TypeFactory.defaultInstance().constructType(getJavaType().getType()));
        } catch (IOException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }
    
}
