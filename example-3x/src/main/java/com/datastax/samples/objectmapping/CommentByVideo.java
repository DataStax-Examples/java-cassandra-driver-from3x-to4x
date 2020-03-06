package com.datastax.samples.objectmapping;

import java.util.UUID;

import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.datastax.samples.ExampleSchema;

/**
 * Specialization for USER.
 */
@Table(
    keyspace = ExampleSchema.KEYSPACE_NAME, 
    name     = ExampleSchema.COMMENT_BY_VIDEO_TABLENAME)
public class CommentByVideo extends Comment {
    
    /** Default constructor. */
    public CommentByVideo() {}
    
    /**
     * Copy constructor.
     *
     * @param c
     */
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
