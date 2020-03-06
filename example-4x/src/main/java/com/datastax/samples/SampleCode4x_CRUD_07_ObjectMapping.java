package com.datastax.samples;

import static com.datastax.samples.ExampleUtils.closeSession;
import static com.datastax.samples.ExampleUtils.connect;
import static com.datastax.samples.ExampleUtils.createKeyspace;
import static com.datastax.samples.ExampleUtils.createTableCommentByUser;
import static com.datastax.samples.ExampleUtils.createTableCommentByVideo;
import static com.datastax.samples.ExampleUtils.truncateTable;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.samples.objectmapping.Comment;
import com.datastax.samples.objectmapping.CommentByUser;
import com.datastax.samples.objectmapping.CommentByVideo;
import com.datastax.samples.objectmapping.CommentDao;
import com.datastax.samples.objectmapping.CommentDaoMapper;

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
public class SampleCode4x_CRUD_07_ObjectMapping implements ExampleSchema {
    
    /** Logger for the class. */
    private static Logger LOGGER = LoggerFactory.getLogger(SampleCode4x_CRUD_07_ObjectMapping.class);
    
    // This will be used as singletons for the sample
    private static CqlSession session;
   
    /** StandAlone (vs JUNIT) to help you running. */
    public static void main(String[] args) {
        
        try {
            
            // Create killrvideo keyspace (if needed)
            createKeyspace();
            
            // Initialize Cluster and Session Objects 
            session = connect();
   
            // Create working table User (if needed)
            createTableCommentByUser(session);
            createTableCommentByVideo(session);
            
            // Comments are used in 2 queries, we need 2 tables to store it
            truncateTable(session, COMMENT_BY_USER_TABLENAME);
            truncateTable(session, COMMENT_BY_VIDEO_TABLENAME);
   
            // All logic is defined in Mapper/Dao/Entities in objectmapping package
            // Mapper required the table to exist
            CommentDao dao = CommentDaoMapper.builder(session)
                    .withDefaultKeyspace(KEYSPACE_NAME)
                    .build().commentDao();
            
            // DataSet
            UUID user_1    = UUID.randomUUID();UUID user_2    = UUID.randomUUID();
            UUID videoid_1 = UUID.randomUUID();UUID videoid_2 = UUID.randomUUID();
            Comment c1 = new Comment(user_1, videoid_1, "I am user1 and video1 is good");
            Comment c2 = new Comment(user_2, videoid_1, "I am user2 and video1 is bad");
            Comment c3 = new Comment(user_1, videoid_2, "Video2 is cool");
            Comment c4 = new Comment(user_2, videoid_2,  "Video2");
            
            /* ==================== CREATE =====================
             * Create comment (in 2 tables with BATCH)
             * ================================================= */
            dao.upsert(c1);dao.upsert(c2);
            dao.upsert(c3);dao.upsert(c4);
            dao.retrieveVideoComments(videoid_2).all()
               .stream().map(CommentByVideo::getComment)
               .forEach(LOGGER::info);
            
            /* =============== UPDATE ==========================
             * == Update one comment (in 2 tables with BATCH) ==
             * ================================================= */
            c1.setComment("This is my new comment");
            dao.upsert(c1);
            dao.retrieveVideoComments(videoid_1).all()
               .stream().map(CommentByVideo::getComment)
               .forEach(LOGGER::info);
            
            /* =============== DELETE ===========================
             * Delete one comment (in 2 tables with BATCH)     ==
             * Note that commentId is NOT ENOUGH as userid and ==
             * videoid are part of the the primary keys.       ==
             * ==================================================*/
            dao.delete(c1);
            dao.retrieveVideoComments(videoid_1).all()
               .stream().map(CommentByVideo::getComment)
               .forEach(LOGGER::info);
            
            /* 
             * ============================ READ ================================
             * == Query1: List comments for user_1 with table comment_by_user   =
             * == Query2: List comments for video_2 with table comment_by_video =
             * ==================================================================
             */
            
            dao.retrieveUserComments(videoid_2).all()
               .stream().map(CommentByUser::getComment)
               .forEach(LOGGER::info);
            
            dao.retrieveVideoComments(videoid_2).all()
               .stream().map(CommentByVideo::getComment)
               .forEach(LOGGER::info);
                        
        } finally {
            // Close Cluster and Session 
            closeSession(session);
        }
        System.exit(0);
    }
    
}
