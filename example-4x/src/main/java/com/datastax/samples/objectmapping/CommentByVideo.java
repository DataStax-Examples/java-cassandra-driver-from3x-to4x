package com.datastax.samples.objectmapping;

import java.util.UUID;

import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.datastax.samples.ExampleSchema;

/**
 * Specialization for VIDEO.
 *
 * @author DataStax Developer Advocates team.
 */
@Entity
@CqlName(ExampleSchema.COMMENT_BY_VIDEO_TABLENAME)
public class CommentByVideo extends Comment {
    
    /** Serial. */
    private static final long serialVersionUID = -6738790629520080307L;
    
    public CommentByVideo() {
    }
    
    public CommentByVideo(Comment c) {
        this.commentid  = c.getCommentid();
        this.userid     = c.getUserid();
        this.videoid    = c.getVideoid();
        this.comment    = c.getComment();
    }

    /**
     * Getter for attribute 'videoid'.
     *
     * @return
     *       current value of 'videoid'
     */
    @PartitionKey
    public UUID getVideoid() {
        return videoid;
    }
    
    
}
