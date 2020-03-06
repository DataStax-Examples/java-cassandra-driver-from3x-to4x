package com.datastax.samples.codec;

import java.nio.ByteBuffer;

import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.samples.ExampleSchema;
import com.datastax.samples.dto.VideoFormatDto;

/**
 * Codec to help with UDT and do not use only UDTValue.
 */
public class VideoFormatDtoCodec extends TypeCodec<VideoFormatDto> implements ExampleSchema {

    private final TypeCodec<UDTValue> innerCodec;

    private final UserType videoFormatUdt;

    public VideoFormatDtoCodec(TypeCodec<UDTValue> innerCodec, Class<VideoFormatDto> javaType) {
        super(innerCodec.getCqlType(), javaType);
        this.innerCodec     = innerCodec;
        this.videoFormatUdt = (UserType) innerCodec.getCqlType();
    }
    
    /** {@inheritDoc} */
    @Override
    public ByteBuffer serialize(VideoFormatDto value, ProtocolVersion protocolVersion) throws InvalidTypeException {
        return innerCodec.serialize(toUDTValue(value), protocolVersion);
    }

    @Override
    public VideoFormatDto deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException {
        return toVideoFormatDto(innerCodec.deserialize(bytes, protocolVersion));
    }
    
    protected VideoFormatDto toVideoFormatDto(UDTValue value) {
        return value == null ? null : new VideoFormatDto(
                value.getInt(UDT_VIDEO_FORMAT_WIDTH),
                value.getInt(UDT_VIDEO_FORMAT_HEIGHT)
        );
    }

    /** {@inheritDoc} */
    @Override
    public VideoFormatDto parse(String value) throws InvalidTypeException {
        return value == null || value.isEmpty() || value.equalsIgnoreCase("NULL") ? 
                null : toVideoFormatDto(innerCodec.parse(value));
    }

    /** {@inheritDoc} */
    @Override
    public String format(VideoFormatDto value) throws InvalidTypeException {
        return value == null ? "NULL" : innerCodec.format(toUDTValue(value));
    }
    
    protected UDTValue toUDTValue(VideoFormatDto value) {
        return value == null ? null : videoFormatUdt.newValue()
                .setInt(UDT_VIDEO_FORMAT_WIDTH,  value.getWidth())
                .setInt(UDT_VIDEO_FORMAT_HEIGHT, value.getHeight());
    }
    

}
