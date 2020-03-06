package com.datastax.samples;

import static com.datastax.samples.ExampleUtils.closeSessionAndCluster;
import static com.datastax.samples.ExampleUtils.connect;
import static com.datastax.samples.ExampleUtils.createKeyspace;
import static com.datastax.samples.ExampleUtils.createTableCommentByUser;
import static com.datastax.samples.ExampleUtils.createTableCommentByVideo;
import static com.datastax.samples.ExampleUtils.truncateTable;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.utils.UUIDs;

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
public class SampleCode3x_CRUD_03_Batches implements ExampleSchema {
    
    /** Logger for the class. */
    private static Logger LOGGER = LoggerFactory.getLogger(SampleCode3x_CRUD_03_Batches.class);
    
    // This will be used as singletons for the sample
    private static Cluster cluster;
    private static Session session;
    
    // Prepare your statements once and execute multiple times 
    private static PreparedStatement insertIntoCommentByVideo;
    private static PreparedStatement insertIntoCommentByUser;
    private static PreparedStatement deleteCommentByVideo;
    private static PreparedStatement deleteCommentByUser;
    private static PreparedStatement selectCommentByVideo;
    private static PreparedStatement selectCommentByUser;
    
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

            // Prepare your statements once and execute multiple times 
            prepareStatements();

            // Will use this identifiers is all tests
            UUID user_1    = UUIDs.random();UUID user_2    = UUIDs.random();
            UUID videoid_1 = UUIDs.random();UUID videoid_2 = UUIDs.random();
            
            /* ==================== CREATE =====================
             * Create comment (in 2 tables with BATCH)
             * ================================================= */
            
            UUID commentId11 = createComment(user_1, videoid_1, "I am user1 and video1 is good");
            UUID commentId21 = createComment(user_2, videoid_1, "I am user2 and video2 is bad");
            createComment(user_1, videoid_2, "Video2 is cool");
            createComment(user_2, videoid_2, "Video2");
            retrieveCommentsVideo(videoid_2).stream().forEach(LOGGER::info);
            
            /* =============== UPDATE ==========================
             * == Update one comment (in 2 tables with BATCH) ==
             * ================================================= */
            
            updateComment(commentId11, user_1, videoid_1, "This is my new comment");
            retrieveCommentsVideo(videoid_1).stream().forEach(LOGGER::info);
            
            /* =============== DELETE ===========================
             * Delete one comment (in 2 tables with BATCH)     ==
             * Note that commentId is NOT ENOUGH as userid and ==
             * videoid are part of the the primary keys.       ==
             * ==================================================*/
            
            deleteComment(commentId21, user_2, videoid_1);
            retrieveCommentsVideo(videoid_1).stream().forEach(LOGGER::info);
            
            /* 
             * ============================ READ ================================
             * == Query1: List comments for user_1 with table comment_by_user   =
             * == Query2: List comments for video_2 with table comment_by_video =
             * ==================================================================
             */
            
            retrieveCommentsUser(user_1).stream().forEach(LOGGER::info);
            retrieveCommentsVideo(videoid_2).stream().forEach(LOGGER::info);
            
        } finally {
            // Close Cluster and Session 
            closeSessionAndCluster(session, cluster);
        }
        System.exit(0);
    }
    
    private static void prepareStatements() {
        insertIntoCommentByVideo = session.prepare(
                QueryBuilder.insertInto(COMMENT_BY_VIDEO_TABLENAME)
                            .value(COMMENT_BY_VIDEO_VIDEOID,   QueryBuilder.bindMarker())
                            .value(COMMENT_BY_VIDEO_USERID,    QueryBuilder.bindMarker())
                            .value(COMMENT_BY_VIDEO_COMMENTID, QueryBuilder.bindMarker())
                            .value(COMMENT_BY_VIDEO_COMMENT,   QueryBuilder.bindMarker()));
        insertIntoCommentByUser = session.prepare(
                QueryBuilder.insertInto(COMMENT_BY_USER_TABLENAME)
                            .value(COMMENT_BY_USER_USERID,     QueryBuilder.bindMarker())
                            .value(COMMENT_BY_USER_VIDEOID,    QueryBuilder.bindMarker())
                            .value(COMMENT_BY_USER_COMMENTID,  QueryBuilder.bindMarker())
                            .value(COMMENT_BY_USER_COMMENT,    QueryBuilder.bindMarker()));
        deleteCommentByUser = session.prepare(
                QueryBuilder.delete().from(COMMENT_BY_USER_TABLENAME)
                .where(QueryBuilder.eq(COMMENT_BY_USER_USERID, QueryBuilder.bindMarker()))
                .and(QueryBuilder.eq(COMMENT_BY_USER_COMMENTID, QueryBuilder.bindMarker())));
        deleteCommentByVideo = session.prepare(
                QueryBuilder.delete().from(COMMENT_BY_VIDEO_TABLENAME)
                .where(QueryBuilder.eq(COMMENT_BY_USER_VIDEOID, QueryBuilder.bindMarker()))
                .and(QueryBuilder.eq(COMMENT_BY_USER_COMMENTID, QueryBuilder.bindMarker())));
        selectCommentByVideo = session.prepare(
                QueryBuilder.select().from(COMMENT_BY_VIDEO_TABLENAME)
                .where(QueryBuilder.eq(COMMENT_BY_USER_VIDEOID, QueryBuilder.bindMarker())));
        selectCommentByUser  = session.prepare(
                QueryBuilder.select().from(COMMENT_BY_USER_TABLENAME)
                .where(QueryBuilder.eq(COMMENT_BY_USER_USERID, QueryBuilder.bindMarker())));
    }

    
    private static UUID createComment(UUID userid, UUID videoid, String comment) {
        UUID commentid = UUIDs.timeBased();
        updateComment(commentid, userid, videoid, comment);
        return commentid;
    }
     
    private static void updateComment(UUID commentid, UUID userid, UUID videoid, String comment) {
         final BatchStatement batch = new BatchStatement();
         batch.add(insertIntoCommentByVideo.bind(videoid, userid, commentid, comment));
         batch.add(insertIntoCommentByUser.bind(userid, videoid, commentid, comment));
         session.execute(batch);
    }
    
    private static void deleteComment(UUID commentid, UUID userid, UUID videoid) {
        session.execute(new BatchStatement()
                .add(deleteCommentByUser.bind(userid, commentid))
                .add(deleteCommentByVideo.bind(videoid, commentid)));
    }
    
    private static List<String> retrieveCommentsVideo(UUID videoid) {
        return session.execute(selectCommentByVideo.bind(videoid))
               .all().stream().map(row -> row.getString(COMMENT_BY_VIDEO_COMMENT))
               .collect(Collectors.toList());
    }
    
    private static List<String> retrieveCommentsUser(UUID userId) {
        return session.execute(selectCommentByUser.bind(userId))
                .all().stream().map(row -> row.getString(COMMENT_BY_USER_COMMENT))
                .collect(Collectors.toList());
    }
    
}

