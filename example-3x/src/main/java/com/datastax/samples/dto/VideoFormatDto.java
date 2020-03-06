package com.datastax.samples.dto;

/**
 * CREATE TYPE IF NOT EXISTS video_format (
 *   width   int,
 *   height  int,
 *   frames  list<int>
 * );
 */
public class VideoFormatDto {
    
    private int width = 0;
    
    private int height = 0;
    
    public VideoFormatDto() {}

    public VideoFormatDto(int w, int h) {
        this.width = w;
        this.height = h;
    }

    public int getWidth() {
        return width;
    }

    public void setWidth(int width) {
        this.width = width;
    }

    public int getHeight() {
        return height;
    }

    public void setHeight(int height) {
        this.height = height;
    }

    @Override
    public String toString() {
        return "VideoFormatDto [width=" + width + ", height=" + height;
    }
}
