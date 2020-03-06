package com.datastax.samples.objectmapping;

import java.util.UUID;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.mapper.MapperContext;
import com.datastax.oss.driver.api.mapper.entity.EntityHelper;
import com.datastax.oss.driver.api.mapper.entity.saving.NullSavingStrategy;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.samples.ExampleSchema;

/**
 * Query implementation for Comment Dse and Mapper.
 *
 * @author DataStax Developer Advocates team.
 */
public class CommentDaoQueryProvider implements CommentDao, ExampleSchema {

    private final CqlSession cqlSession;
    
    private final EntityHelper<CommentByUser>  helperUser;
    private final EntityHelper<CommentByVideo> helperVideo;
    
    private static PreparedStatement selectCommentByVideo;
    private static PreparedStatement selectCommentByUser;
    
    private PreparedStatement psInsertCommentUser;
    private PreparedStatement psDeleteCommentUser;
    private PreparedStatement psInsertCommentVideo;
    private PreparedStatement psDeleteCommentVideo;
    
    public CommentDaoQueryProvider(MapperContext context,
            EntityHelper<CommentByUser> helperUser,
            EntityHelper<CommentByVideo> helperVideo) {
        
        this.cqlSession                    = context.getSession();
        this.helperUser      = helperUser;
        this.helperVideo     = helperVideo;
        psInsertCommentUser  = cqlSession.prepare(helperUser.insert().asCql());
        psDeleteCommentUser  = cqlSession.prepare(helperUser.deleteByPrimaryKey().asCql());
        psInsertCommentVideo = cqlSession.prepare(helperVideo.insert().asCql());
        psDeleteCommentVideo = cqlSession.prepare(helperVideo.deleteByPrimaryKey().asCql());
        
        selectCommentByVideo = cqlSession.prepare(
                QueryBuilder.selectFrom(COMMENT_BY_VIDEO_TABLENAME).all()
                .whereColumn(COMMENT_BY_VIDEO_VIDEOID).isEqualTo(QueryBuilder.bindMarker())
                .build());
        selectCommentByUser  = cqlSession.prepare(
                QueryBuilder.selectFrom(COMMENT_BY_USER_TABLENAME).all()
                .whereColumn(COMMENT_BY_USER_USERID).isEqualTo(QueryBuilder.bindMarker())
                .build());
    }
    
    /** {@inheritDoc} */
    @Override
    public PagingIterable<CommentByUser> retrieveUserComments(UUID userid) {
        return cqlSession.execute(selectCommentByUser.bind(userid)).map(helperUser::get);
    }

    /** {@inheritDoc} */
    @Override
    public PagingIterable<CommentByVideo> retrieveVideoComments(UUID videoid) {
        return cqlSession.execute(selectCommentByVideo.bind(videoid)).map(helperVideo::get);
    }
    
    /** {@inheritDoc} */
    @Override
    public void upsert(Comment comment) {
        cqlSession.execute(BatchStatement.builder(DefaultBatchType.LOGGED)
                .addStatement(bind(psInsertCommentUser,  new CommentByUser(comment),  helperUser))
                .addStatement(bind(psInsertCommentVideo, new CommentByVideo(comment), helperVideo))
                .build());
    }
    
    /** {@inheritDoc} */
    @Override
    public void delete(Comment comment) {
        
        CommentByUser cbu = new CommentByUser();
        cbu.setCommentid(comment.getCommentid());
        cbu.setUserid(comment.getUserid());
        
        CommentByVideo cbv = new CommentByVideo();
        cbv.setCommentid(comment.getCommentid());
        cbv.setVideoid(comment.getVideoid());
        
        cqlSession.execute(
                BatchStatement.builder(DefaultBatchType.LOGGED)
                    .addStatement(bind(psDeleteCommentUser,  cbu, helperUser))
                    .addStatement(bind(psDeleteCommentVideo, cbv, helperVideo))
                    .build());
    }
    
    public static <T> BoundStatement bind(PreparedStatement preparedStatement, T entity, EntityHelper<T> entityHelper) {
        BoundStatementBuilder boundStatement = preparedStatement.boundStatementBuilder();
        entityHelper.set(entity, boundStatement, NullSavingStrategy.DO_NOT_SET);
        return boundStatement.build();
    }
    
}
