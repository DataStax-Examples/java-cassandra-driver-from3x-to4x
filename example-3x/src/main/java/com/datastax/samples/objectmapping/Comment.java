package com.datastax.samples.objectmapping;

import java.util.Date;
import java.util.UUID;

import com.datastax.driver.core.utils.UUIDs;
import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Computed;
import com.datastax.samples.ExampleSchema;

/**
 * Bean standing for comment on video.
 *
 * @author DataStax Developer Advocates team.
 */
public class Comment implements ExampleSchema {
    
    @Column(name = COMMENT_BY_USER_USERID)
    protected UUID userid;
    
    @Column(name = COMMENT_BY_USER_VIDEOID)
    protected UUID videoid;

    @Column(name = COMMENT_BY_USER_COMMENTID)
    @ClusteringColumn
    protected UUID commentid;

    @Column(name = COMMENT_BY_USER_COMMENT)
    protected String comment;

    @Computed("toTimestamp(commentid)")
    private Date dateOfComment;
    
    /**
     * Default constructor.
     */
    public Comment() {
    }
    
    public Comment(UUID userid, UUID videoId, String comment) {
        this.userid    = userid;
        this.videoid   = videoId;
        this.comment   = comment;
        this.commentid = UUIDs.timeBased();
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
     *      new value for 'userid '
     */
    public void setUserid(UUID userid) {
        this.userid = userid;
    }

    /**
     * Setter for attribute 'videoid'.
     * @param videoid
     *      new value for 'videoid '
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
     *      new value for 'commentid '
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
     *      new value for 'comment '
     */
    public void setComment(String comment) {
        this.comment = comment;
    }

    /**
     * Getter for attribute 'dateOfComment'.
     *
     * @return
     *       current value of 'dateOfComment'
     */
    public Date getDateOfComment() {
        return dateOfComment;
    }

    /**
     * Setter for attribute 'dateOfComment'.
     * @param dateOfComment
     *      new value for 'dateOfComment '
     */
    public void setDateOfComment(Date dateOfComment) {
        this.dateOfComment = dateOfComment;
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
