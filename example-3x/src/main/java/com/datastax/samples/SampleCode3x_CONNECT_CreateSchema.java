package com.datastax.samples;

import static com.datastax.samples.ExampleUtils.createTableCommentByUser;
import static com.datastax.samples.ExampleUtils.createTableCommentByVideo;
import static com.datastax.samples.ExampleUtils.createTableUser;
import static com.datastax.samples.ExampleUtils.createTableVideo;
import static com.datastax.samples.ExampleUtils.createTableVideoViews;
import static com.datastax.samples.ExampleUtils.createUdtVideoFormat;
import static com.datastax.samples.ExampleUtils.createKeyspace;
import static com.datastax.samples.ExampleUtils.createTableFiles;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

/**
 * Sample code to create tables, types and objects in a keyspace.
 * 
 * Pre-requisites:
 * - Cassandra running locally (127.0.0.1, port 9042)
 */
public class SampleCode3x_CONNECT_CreateSchema implements ExampleSchema {
    
    /** Logger for the class. */
    private static Logger LOGGER = LoggerFactory.getLogger(SampleCode3x_CONNECT_CreateSchema.class);
    
    /** StandAlone (vs JUNIT) to help you running. */
    public static void main(String[] args) {
        
        try(Cluster cluster = Cluster.builder()
                .addContactPoint("127.0.0.1")
                .build()) {
            
            LOGGER.info("Connected to Cluster");
            createKeyspace(cluster);
            
            try(Session session = cluster.connect(KEYSPACE_NAME)) {
                LOGGER.info("[OK] Connected to Keyspace {}", KEYSPACE_NAME);
                createUdtVideoFormat(session);
                createTableUser(session);
                createTableVideo(session);
                createTableVideoViews(session);
                createTableCommentByVideo(session);
                createTableCommentByUser(session);
                createTableFiles(session);
            }
        }
        LOGGER.info("[OK] Success");
        System.exit(0);
    }
   
}
