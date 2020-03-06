package com.datastax.samples;

import static com.datastax.samples.ExampleUtils.closeSessionAndCluster;
import static com.datastax.samples.ExampleUtils.connect;
import static com.datastax.samples.ExampleUtils.createKeyspace;
import static com.datastax.samples.ExampleUtils.createTableVideoViews;
import static com.datastax.samples.ExampleUtils.truncateTable;

import java.util.Optional;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.utils.UUIDs;

/**
 * Working with Counters :
 *
 * CREATE TABLE IF NOT EXISTS videos_views (
 *    videoid     uuid,
 *    views       counter,
 *    PRIMARY KEY (videoid)
 * );
 *
 * Definition:
 * - 64-bit signed integer
 * -First op assumes the value is zero
 * 
 * Use-case:
 * - Imprecise values such as likes, views, etc.
 * 
 * Two operations:
 * - Increment
 * - Decrement
 * 
 * Limitations:
 * - Cannot be part of primary key
 * - Counters not mixed with other types in table
 * - Value cannot be set
 * - Rows with counters cannot be inserted
 * - Updates are not idempotent
 * - Counters should not be used for precise values
 */
public class SampleCode3x_CRUD_08_Counters implements ExampleSchema {

    /** Logger for the class. */
    private static Logger LOGGER = LoggerFactory.getLogger(SampleCode3x_CRUD_08_Counters.class);

    // This will be used as singletons for the sample
    private static Cluster cluster;
    private static Session session;
    
    // Prepare your statements once and execute multiple times 
    private static PreparedStatement stmtIncrement;
    private static PreparedStatement stmtDecrement;
    private static PreparedStatement stmtFindById;
    private static PreparedStatement stmtDelete;
    
    /** StandAlone (vs JUNIT) to help you running. */
    public static void main(String[] args) {
        try {

            // Create killrvideo keyspace (if needed)
            createKeyspace();
            
            // Initialize Cluster and Session Objects 
            session = connect(cluster);
            
            // Create tables for tests
            createTableVideoViews(session);
            
            // Empty tables for tests
            truncateTable(session, VIDEO_VIEWS_TABLENAME);

            // Prepare your statements once and execute multiple times 
            prepareStatements();
           
            // ========= CREATE ============

            // We cannot insert in a table with a counter
            UUID videoId = UUIDs.random();
            LOGGER.info("+ Video views {}", findById(videoId));
            
            // ========= UPDATE ============
            
            incrementBy(videoId, 10);
            LOGGER.info("+ Video views : {}", findById(videoId).get());
            
            decrementBy(videoId, 8);
            LOGGER.info("+ Video views : {}", findById(videoId).get());
            
            // ========= DELETE ============
            
            delete(videoId);
            LOGGER.info("+ Video views {}", findById(videoId));
            
           
        } finally {
            // Close Cluster and Session 
            closeSessionAndCluster(session, cluster);
        }
        System.exit(0);
    }
    
    private static Optional <Long> findById(UUID videoid) {
        Row record = session.execute(stmtFindById.bind(videoid)).one();
        if (null != record) {
            return Optional.of(record.getLong(VIDEO_VIEWS_VIEWS));
        }
        return Optional.empty();
    }
    
    private static void incrementBy(UUID videoid, long val) {
        session.execute(stmtIncrement.bind(val, videoid));
    }
    
    private static void decrementBy(UUID videoid, long val) {
        session.execute(stmtDecrement.bind(val, videoid));
    }
    
    private static void delete(UUID videoid) {
        session.execute(stmtDelete.bind(videoid));
    }
    
    private static void prepareStatements() {

        // update videos_views SET views =  views + X WHERE videoid=... 
        stmtIncrement = session.prepare(QueryBuilder
                .update(VIDEO_VIEWS_TABLENAME)
                .with(QueryBuilder.incr(VIDEO_VIEWS_VIEWS, QueryBuilder.bindMarker()))
                .where(QueryBuilder.eq(VIDEO_VIEWS_VIDEOID, QueryBuilder.bindMarker())));

        // update videos_views SET views =  views + X WHERE videoid=..
        stmtDecrement = session.prepare(QueryBuilder
                .update(VIDEO_VIEWS_TABLENAME)
                .with(QueryBuilder.decr(VIDEO_VIEWS_VIEWS, QueryBuilder.bindMarker()))
                .where(QueryBuilder.eq(VIDEO_VIEWS_VIDEOID, QueryBuilder.bindMarker())));
        
        // SELECT views FROM videos_views WHERE videoid=... 
        stmtFindById = session.prepare(QueryBuilder
                .select().column(VIDEO_VIEWS_VIEWS)
                .from(VIDEO_VIEWS_TABLENAME)
                .where(QueryBuilder.eq(VIDEO_VIEWS_VIDEOID, QueryBuilder.bindMarker())));
        
        // DELETE FROM videos_views WHERE videoid=... 
        stmtDelete = session.prepare(QueryBuilder
                .delete().from(VIDEO_VIEWS_TABLENAME)
                .where(QueryBuilder.eq(VIDEO_VIEWS_VIDEOID, QueryBuilder.bindMarker())));
    }

}
