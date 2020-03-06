package com.datastax.samples.codec;

import java.nio.ByteBuffer;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.samples.ExampleSchema;
import com.datastax.samples.dto.VideoFormatDto;

/**
 * Codec.
 */
public class UdtVideoFormatCodec implements TypeCodec<VideoFormatDto>, ExampleSchema {

    final TypeCodec<UdtValue> innerCodec;
    
    final UserDefinedType videoFormatUdt;

    public UdtVideoFormatCodec(TypeCodec<UdtValue> innerCodec, Class<VideoFormatDto> javaType) {
        this.innerCodec     = innerCodec;
        this.videoFormatUdt = (UserDefinedType) innerCodec.getCqlType();
    }
    
    /** {@inheritDoc} */
    @Override
    public GenericType<VideoFormatDto> getJavaType() {
        return GenericType.of(VideoFormatDto.class);
    }

    /** {@inheritDoc} */
    @Override
    public DataType getCqlType() {
        return videoFormatUdt;
    }

    /** {@inheritDoc} */
    @Override
    public ByteBuffer encode(VideoFormatDto value, ProtocolVersion protocolVersion) {
        return innerCodec.encode(toUDTValue(value), protocolVersion);
    }

    /** {@inheritDoc} */
    @Override
    public VideoFormatDto decode(ByteBuffer bytes, ProtocolVersion protocolVersion) {
        return toVideoFormatDto(innerCodec.decode(bytes, protocolVersion));
    }

    /** {@inheritDoc} */
    @Override
    public String format(VideoFormatDto value) {
        return value == null ? "NULL" : innerCodec.format(toUDTValue(value));
    }

    /** {@inheritDoc} */
    @Override
    public VideoFormatDto parse(String value) {
        return value == null || value.isEmpty() || value.equalsIgnoreCase("NULL") ? 
                null : toVideoFormatDto(innerCodec.parse(value));
    }
    
    protected VideoFormatDto toVideoFormatDto(UdtValue value) {
        return value == null ? null : new VideoFormatDto(
                value.getInt(UDT_VIDEO_FORMAT_WIDTH),
                value.getInt(UDT_VIDEO_FORMAT_HEIGHT)
        );
    }
    
    protected UdtValue toUDTValue(VideoFormatDto value) {
        return value == null ? null : videoFormatUdt.newValue()
                .setInt(UDT_VIDEO_FORMAT_WIDTH,  value.getWidth())
                .setInt(UDT_VIDEO_FORMAT_HEIGHT, value.getHeight());
    }

}
