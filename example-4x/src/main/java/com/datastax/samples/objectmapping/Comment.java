package com.datastax.samples.objectmapping;

import java.io.Serializable;
import java.util.UUID;

import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.datastax.oss.driver.api.mapper.annotations.ClusteringColumn;
import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.samples.ExampleSchema;

/**
 * Bean standing for comment on video.
 *
 * @author DataStax Developer Advocates team.
 */
public class Comment implements Serializable, ExampleSchema {
    
    /** Serial. */
    private static final long serialVersionUID = 7675521710612951368L;
    
    @CqlName(COMMENT_BY_USER_USERID)
    protected UUID userid;
    
    @CqlName(COMMENT_BY_USER_VIDEOID)
    protected UUID videoid;

    @ClusteringColumn
    @CqlName(COMMENT_BY_USER_COMMENTID)
    protected UUID commentid;
    
    @CqlName(COMMENT_BY_USER_COMMENT)
    protected String comment;
    
    /**
     * Default constructor.
     */
    public Comment() {
    }
    
    /**
     * Constructor with parameters.
     *
     * @param userid
     *      user unique identifier
     * @param videoId
     *      video unique identifier
     * @param comment
     *      text value for the comment
     */
    public Comment(UUID userid, UUID videoId, String comment) {
        this.userid    = userid;
        this.videoid   = videoId;
        this.comment   = comment;
        this.commentid = Uuids.timeBased();
    }
    
    /**
     * Default constructor.
     */
    public Comment(String comment) {
        this.comment = comment;
    }
    
    /**
     * Setter for attribute 'userid'.
     * @param userid
     * 		new value for 'userid '
     */
    public void setUserid(UUID userid) {
        this.userid = userid;
    }

    /**
     * Setter for attribute 'videoid'.
     * @param videoid
     * 		new value for 'videoid '
     */
    public void setVideoid(UUID videoid) {
        this.videoid = videoid;
    }

    /**
     * Getter for attribute 'commentid'.
     *
     * @return
     *       current value of 'commentid'
     */
    public UUID getCommentid() {
        return commentid;
    }

    /**
     * Setter for attribute 'commentid'.
     * @param commentid
     * 		new value for 'commentid '
     */
    public void setCommentid(UUID commentid) {
        this.commentid = commentid;
    }

    /**
     * Getter for attribute 'comment'.
     *
     * @return
     *       current value of 'comment'
     */
    public String getComment() {
        return comment;
    }

    /**
     * Setter for attribute 'comment'.
     * @param comment
     * 		new value for 'comment '
     */
    public void setComment(String comment) {
        this.comment = comment;
    }
    
    /**
     * Getter for attribute 'userid'.
     *
     * @return
     *       current value of 'userid'
     */
    public UUID getUserid() {
        return userid;
    }
    
    /**
     * Getter for attribute 'videoid'.
     *
     * @return
     *       current value of 'videoid'
     */
    public UUID getVideoid() {
        return videoid;
    }
    

}
