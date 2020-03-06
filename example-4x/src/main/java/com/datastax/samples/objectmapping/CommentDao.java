package com.datastax.samples.objectmapping;

import java.util.UUID;

import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.Query;
import com.datastax.oss.driver.api.mapper.annotations.QueryProvider;
import com.datastax.samples.ExampleSchema;

/**
 * Implementation of Services to work with Comments in Killrvideo. We work with 
 * 2 tables 'comments_by_user' and 'comments_by_video'.
 */ 
@Dao
public interface CommentDao extends ExampleSchema {
    
    @Query("SELECT * FROM ${keyspaceId}.${tableId} "
         + "WHERE " + COMMENT_BY_USER_USERID + " = :userid ")
    PagingIterable<CommentByUser> retrieveUserComments(UUID userid);
    
    @Query("SELECT * FROM ${keyspaceId}.${tableId} "
            + "WHERE " + COMMENT_BY_VIDEO_VIDEOID + " = :videoid ")
    PagingIterable<CommentByVideo> retrieveVideoComments(UUID videoid);
    
    @QueryProvider(
            providerClass = CommentDaoQueryProvider.class,
            entityHelpers = { CommentByUser.class, CommentByVideo.class})
    void upsert(Comment comment);
    
    @QueryProvider(
            providerClass = CommentDaoQueryProvider.class,
            entityHelpers = { CommentByUser.class, CommentByVideo.class})
    void delete(Comment res);
}
