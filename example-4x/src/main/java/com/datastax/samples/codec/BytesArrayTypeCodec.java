package com.datastax.samples.codec;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;

/**
 * Retrieve a blob as a byte array from Cassandra.
 */
public class BytesArrayTypeCodec  implements TypeCodec<byte[]> {

    private Charset charSet = StandardCharsets.UTF_8;
    
    public BytesArrayTypeCodec(Charset charSet) {
        this.charSet = charSet;
    }
    
    /** Default constructor. */
    public BytesArrayTypeCodec() {
        this(StandardCharsets.UTF_8);
    }
    
    /** {@inheritDoc} */
    @Override
    public GenericType<byte[]> getJavaType() {
        return GenericType.of(byte[].class);
    }

    /** {@inheritDoc} */
    @Override
    public DataType getCqlType() {
        return DataTypes.BLOB;
    }

    /** {@inheritDoc} */
    @Override
    public ByteBuffer encode(byte[] value, ProtocolVersion protocolVersion) {
        if (value == null) return null;
        ByteBuffer byteBuffer = ByteBuffer.allocate(value.length);
        byteBuffer.put(value);
        return byteBuffer;
    }

    /** {@inheritDoc} */
    @Override
    public byte[] decode(ByteBuffer byteBuffer, ProtocolVersion protocolVersion) {
        if (byteBuffer == null) return null;
        byte[] bytesArray = new byte[byteBuffer.remaining()];
        byteBuffer.get(bytesArray, 0, bytesArray.length);
        return bytesArray;
    }

    /** {@inheritDoc} */
    @Override
    public String format(byte[] value) {
        if (value == null) return "NULL";
        return new String(value, charSet);
    }

    /** {@inheritDoc} */
    @Override
    public byte[] parse(String value) {
        return (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL"))
                ? null
                : value.getBytes(charSet);
    }

}
