package com.datastax.samples.dto;

import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * CREATE TABLE IF NOT EXISTS videos ( videoid uuid, title text, upload timestamp, email text, url text, tags set <text>, formats
 * map <text,frozen<video_format>>, PRIMARY KEY (videoid) );
 */
public class VideoDto implements Serializable {

    /** Serial. */
    private static final long serialVersionUID = -5086632646056781255L;

    private UUID videoid;
    
    private String title;
    
    private String email;
    
    private String url;
    
    @JsonIgnore
    private long upload = Instant.now().toEpochMilli();
    
    private Set<String> tags = new HashSet<>();
    
    private List<Integer> frames = new ArrayList<>();
    
    private Map<String, VideoFormatDto> formats = new HashMap<>();

    public VideoDto() {}

    public VideoDto(UUID videoId, String title, String email, String url) {
        super();
        this.videoid = videoId;
        this.title = title;
        this.email = email;
    }

    /**
     * Getter accessor for attribute 'videoId'.
     *
     * @return current value of 'videoId'
     */
    public UUID getVideoid() {
        return videoid;
    }

    /**
     * Setter accessor for attribute 'videoId'.
     * 
     * @param videoId
     *            new value for 'videoId '
     */
    public void setVideoid(UUID videoId) {
        this.videoid = videoId;
    }

    /**
     * Getter accessor for attribute 'title'.
     *
     * @return current value of 'title'
     */
    public String getTitle() {
        return title;
    }

    /**
     * Setter accessor for attribute 'title'.
     * 
     * @param title
     *            new value for 'title '
     */
    public void setTitle(String title) {
        this.title = title;
    }

    /**
     * Getter accessor for attribute 'upload'.
     *
     * @return current value of 'upload'
     */
    public Long getUpload() {
        return upload;
    }

    /**
     * Setter accessor for attribute 'upload'.
     * 
     * @param upload
     *            new value for 'upload '
     */
     public void setUpload(Long upload) {
        this.upload = upload;
     }

    /**
     * Getter accessor for attribute 'email'.
     *
     * @return current value of 'email'
     */
    public String getEmail() {
        return email;
    }

    /**
     * Setter accessor for attribute 'email'.
     * 
     * @param email
     *            new value for 'email '
     */
    public void setEmail(String email) {
        this.email = email;
    }

    /**
     * Getter accessor for attribute 'tags'.
     *
     * @return current value of 'tags'
     */
    public Set<String> getTags() {
        return tags;
    }

    /**
     * Setter accessor for attribute 'tags'.
     * 
     * @param tags
     *            new value for 'tags '
     */
    public void setTags(Set<String> tags) {
        this.tags = tags;
    }

    /**
     * Getter accessor for attribute 'formatsd'.
     *
     * @return current value of 'formatsd'
     */
    public Map<String, VideoFormatDto> getFormats() {
        return formats;
    }

    /**
     * Setter accessor for attribute 'formatsd'.
     * 
     * @param formatsd
     *            new value for 'formatsd '
     */
    public void setFormats(Map<String, VideoFormatDto> formatsd) {
        this.formats = formatsd;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    /**
     * Getter accessor for attribute 'frames'.
     *
     * @return current value of 'frames'
     */
    public List<Integer> getFrames() {
        return frames;
    }

    /**
     * Setter accessor for attribute 'frames'.
     * 
     * @param frames
     *            new value for 'frames '
     */
    public void setFrames(List<Integer> frames) {
        this.frames = frames;
    }

}
