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
    name     = ExampleSchema.COMMENT_BY_USER_TABLENAME)
public class CommentByUser extends Comment {
    
    /** Default constructor. */
    public CommentByUser() {}
    
    /**
     * Copy constructor.
     *
     * @param c
     */
    public CommentByUser(Comment c) {
        this.commentid  = c.getCommentid();
        this.userid     = c.getUserid();
        this.videoid    = c.getVideoid();
        this.comment    = c.getComment();
    }

    /**
     * Getter for attribute 'userid'.
     *
     * @return
     *       current value of 'userid'
     */
    @PartitionKey
    public UUID getUserid() {
        return userid;
    }

}
