package com.datastax.samples.codec;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.samples.ExampleSchema;

/**
 * Convert from BLOB <-> byte[]
 */
public class BytesArrayCodec extends TypeCodec<byte[]> implements ExampleSchema {
    
    private Charset charSet = StandardCharsets.UTF_8;
    
    public BytesArrayCodec(Charset charSet) {
        super(DataType.blob(), byte[].class);
        this.charSet = charSet;
    }
    
    /** Default constructor. */
    public BytesArrayCodec() {
        this(StandardCharsets.UTF_8);
    }

    /** {@inheritDoc} */
    @Override
    public byte[] parse(String value) throws InvalidTypeException {
        return (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL"))
                ? null
                : value.getBytes(charSet);
    }
    
    /** {@inheritDoc} */
    @Override
    public String format(byte[] value) throws InvalidTypeException {
        if (value == null) return "NULL";
        return new String(value, charSet);
    }
    
    /** {@inheritDoc} */
    @Override
    public ByteBuffer serialize(byte[] value, ProtocolVersion protocolVersion) throws InvalidTypeException {
        if (value == null) return null;
        ByteBuffer byteBuffer = ByteBuffer.allocate(value.length);
        byteBuffer.put(value);
        return byteBuffer;
    }

    /** {@inheritDoc} */
    @Override
    public byte[] deserialize(ByteBuffer byteBuffer, ProtocolVersion protocolVersion) throws InvalidTypeException {
        if (byteBuffer == null) return null;
        byte[] bytesArray = new byte[byteBuffer.remaining()];
        byteBuffer.get(bytesArray, 0, bytesArray.length);
        return bytesArray;
    }   

}
