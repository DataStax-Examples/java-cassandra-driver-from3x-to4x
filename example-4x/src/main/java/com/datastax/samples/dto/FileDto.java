package com.datastax.samples.dto;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.time.Instant;

/**
 * Sample POJO.
 */
public class FileDto implements Serializable {
    
    /** Serial. */
    private static final long serialVersionUID = 7325306650146053028L;

    private String filename;
    
    private String extension;
    
    private Instant updload;
    
    private ByteBuffer content;
    
    public FileDto() {
    }

    /**
     * Getter accessor for attribute 'filename'.
     *
     * @return
     *       current value of 'filename'
     */
    public String getFilename() {
        return filename;
    }

    /**
     * Setter accessor for attribute 'filename'.
     * @param filename
     * 		new value for 'filename '
     */
    public void setFilename(String filename) {
        this.filename = filename;
    }

    /**
     * Getter accessor for attribute 'extension'.
     *
     * @return
     *       current value of 'extension'
     */
    public String getExtension() {
        return extension;
    }

    /**
     * Setter accessor for attribute 'extension'.
     * @param extension
     * 		new value for 'extension '
     */
    public void setExtension(String extension) {
        this.extension = extension;
    }

    /**
     * Getter accessor for attribute 'updload'.
     *
     * @return
     *       current value of 'updload'
     */
    public Instant getUpload() {
        return updload;
    }

    /**
     * Setter accessor for attribute 'updload'.
     * @param updload
     * 		new value for 'updload '
     */
    public void setUpload(Instant updload) {
        this.updload = updload;
    }

    /**
     * Getter accessor for attribute 'content'.
     *
     * @return
     *       current value of 'content'
     */
    public ByteBuffer getContent() {
        return content;
    }

    /**
     * Setter accessor for attribute 'content'.
     * @param content
     * 		new value for 'content '
     */
    public void setContent(ByteBuffer content) {
        this.content = content;
    }
    
    

}
