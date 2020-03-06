package com.datastax.samples;

import static com.datastax.samples.ExampleUtils.dropTableIfExists;
import static com.datastax.samples.ExampleUtils.dropTypeIffExists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

/**
 * Sample code to create tables, types and objects in a keyspace.
 * 
 * Pre-requisites:
 * - Cassandra running locally (127.0.0.1, port 9042)
 * - Keyspace killrvideo created {@link SampleCode3x_CONNECT_CreateKeyspace}
 */
public class SampleCode3x_CONNECT_DropSchema implements ExampleSchema {
    
    /** Logger for the class. */
    private static Logger LOGGER = LoggerFactory.getLogger(SampleCode3x_CONNECT_DropSchema.class);
    
    /** StandAlone (vs JUNIT) to help you running. */
    public static void main(String[] args) {
        
        try(Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build()) {
            LOGGER.info("Connected to Cluster");
            try(Session session = cluster.connect(KEYSPACE_NAME)) {
                LOGGER.info("[OK] Connected to Keyspace {}", KEYSPACE_NAME);
                dropTableIfExists(session, COMMENT_BY_VIDEO_TABLENAME);
                dropTableIfExists(session, COMMENT_BY_USER_TABLENAME);
                dropTableIfExists(session, VIDEO_VIEWS_TABLENAME);
                dropTableIfExists(session, VIDEO_TABLENAME);
                dropTableIfExists(session, USER_TABLENAME);
                dropTypeIffExists(session, UDT_VIDEO_FORMAT_NAME);
            }
        }
        LOGGER.info("[OK] Success");
        System.exit(0);
    }
    
}
