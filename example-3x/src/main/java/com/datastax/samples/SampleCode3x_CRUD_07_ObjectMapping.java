package com.datastax.samples;

import static com.datastax.samples.ExampleUtils.closeSessionAndCluster;
import static com.datastax.samples.ExampleUtils.connect;
import static com.datastax.samples.ExampleUtils.createKeyspace;
import static com.datastax.samples.ExampleUtils.createTableCommentByUser;
import static com.datastax.samples.ExampleUtils.createTableCommentByVideo;
import static com.datastax.samples.ExampleUtils.truncateTable;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.utils.UUIDs;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.Result;
import com.datastax.samples.objectmapping.Comment;
import com.datastax.samples.objectmapping.CommentByUser;
import com.datastax.samples.objectmapping.CommentByVideo;

/**
 * Sample codes using Cassandra OSS Driver 3.x
 * 
 * Disclaimers:
 *  - Tests for arguments nullity has been removed for code clarity
 *  - Packaged as a main class for usability
 *  
 * Pre-requisites:
 * - Cassandra running locally (127.0.0.1, port 9042)
 * 
 * @author Cedrick LUNVEN (@clunven)
 * @author Erick  RAMIREZ (@@flightc)
 */
public class SampleCode3x_CRUD_07_ObjectMapping implements ExampleSchema {
    
    /** Logger for the class. */
    private static Logger LOGGER = LoggerFactory.getLogger(SampleCode3x_CRUD_07_ObjectMapping.class);
    
    // This will be used as singletons for the sample
    private static Cluster cluster;
    private static Session session;
    
    // Prepare your statements once and execute multiple times 
    private static PreparedStatement selectCommentByVideo;
    private static PreparedStatement selectCommentByUser;
    
    // Using Object Mapping.
    private static Mapper < CommentByUser >  mapperCommentByUser;
    private static Mapper < CommentByVideo > mapperCommentByVideo;
    
    /** StandAlone (vs JUNIT) to help you running. */
    public static void main(String[] args) {
        
        try {
            
            // Create killrvideo keyspace (if needed)
            createKeyspace();
            
            // Initialize Cluster and Session Objects 
            session = connect(cluster);
   
            // Create working table User (if needed)
            createTableCommentByUser(session);
            createTableCommentByVideo(session);
            
            // Comments are used in 2 queries, we need 2 tables to store it
            truncateTable(session, COMMENT_BY_USER_TABLENAME);
            truncateTable(session, COMMENT_BY_VIDEO_TABLENAME);
   
            // Mapping Management Object <-> TABLE
            MappingManager mm =  new MappingManager(session);
            mapperCommentByUser  = mm.mapper(CommentByUser.class);
            mapperCommentByVideo = mm.mapper(CommentByVideo.class);

            // Prepare your statements once and execute multiple times 
            prepareStatements();
           
            // DataSet
            UUID user_1    = UUIDs.random();UUID user_2    = UUIDs.random();
            UUID videoid_1 = UUIDs.random();UUID videoid_2 = UUIDs.random();
            Comment c1 = new Comment(user_1, videoid_1, "I am user1 and video1 is good");
            Comment c2 = new Comment(user_2, videoid_1, "I am user2 and video1 is bad");
            Comment c3 = new Comment(user_1, videoid_2, "Video2 is cool");
            Comment c4 = new Comment(user_2, videoid_2,  "Video2");
            
            /* ==================== CREATE =====================
             * Create comment (in 2 tables with BATCH)
             * ================================================= */
            createComment(c1);createComment(c2);
            createComment(c3);createComment(c4);
            
            retrieveCommentsVideo(videoid_2).all()
                    .stream().map(CommentByVideo::getComment)
                    .forEach(LOGGER::info);
            
            /* =============== UPDATE ==========================
             * == Update one comment (in 2 tables with BATCH) ==
             * ================================================= */
            c1.setComment("This is my new comment");
            updateComment(c1);
            retrieveCommentsVideo(videoid_1).all()
                    .stream().map(CommentByVideo::getComment)
                    .forEach(LOGGER::info);
            
            /* =============== DELETE ===========================
             * Delete one comment (in 2 tables with BATCH)     ==
             * Note that commentId is NOT ENOUGH as userid and ==
             * videoid are part of the the primary keys.       ==
             * ==================================================*/
            deleteComment(c1);
            retrieveCommentsVideo(videoid_1).all()
                .stream().map(CommentByVideo::getComment)
                .forEach(LOGGER::info);
            
            /* 
             * ============================ READ ================================
             * == Query1: List comments for user_1 with table comment_by_user   =
             * == Query2: List comments for video_2 with table comment_by_video =
             * ==================================================================
             */
            
            retrieveCommentsUser(user_1).all()
                .stream().map(CommentByUser::getComment)
                .forEach(LOGGER::info);
            
            retrieveCommentsVideo(videoid_2).all()
                .stream().map(CommentByVideo::getComment)
                .forEach(LOGGER::info);
            
        } finally {
            // Close Cluster and Session 
            closeSessionAndCluster(session, cluster);
        }
        System.exit(0);
    }
    
    private static void prepareStatements() {
        selectCommentByVideo = session.prepare(
                QueryBuilder.select().from(COMMENT_BY_VIDEO_TABLENAME)
                .where(QueryBuilder.eq(COMMENT_BY_USER_VIDEOID, QueryBuilder.bindMarker())));
        selectCommentByUser  = session.prepare(
                QueryBuilder.select().from(COMMENT_BY_USER_TABLENAME)
                .where(QueryBuilder.eq(COMMENT_BY_USER_USERID, QueryBuilder.bindMarker())));
    }

    
    private static void createComment(Comment comment) {
        updateComment(comment);
    }
     
    private static void updateComment(Comment comment) {
         final BatchStatement batch = new BatchStatement(BatchStatement.Type.UNLOGGED);
         batch.add(mapperCommentByVideo.saveQuery(new CommentByVideo(comment)));
         batch.add(mapperCommentByUser.saveQuery(new CommentByUser(comment)));
         session.execute(batch);
    }
    
    private static void deleteComment(Comment comment) {
        session.execute(new BatchStatement()
                .add(mapperCommentByVideo.deleteQuery(new CommentByVideo(comment)))
                .add(mapperCommentByVideo.deleteQuery(new CommentByVideo(comment))));
    }
    
    private static Result<CommentByVideo> retrieveCommentsVideo(UUID videoid) {
        return mapperCommentByVideo.map(
                session.execute(selectCommentByVideo.bind(videoid)));
    }
    
    private static  Result<CommentByUser> retrieveCommentsUser(UUID userId) {
        return mapperCommentByUser.map(
                session.execute(selectCommentByUser.bind(userId)));
    }
}
