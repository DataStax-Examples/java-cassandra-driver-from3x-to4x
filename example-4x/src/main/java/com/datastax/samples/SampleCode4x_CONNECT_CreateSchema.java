package com.datastax.samples;

import static com.datastax.samples.ExampleUtils.createTableCommentByUser;
import static com.datastax.samples.ExampleUtils.createTableCommentByVideo;
import static com.datastax.samples.ExampleUtils.createTableUser;
import static com.datastax.samples.ExampleUtils.createTableVideo;
import static com.datastax.samples.ExampleUtils.createTableVideoViews;
import static com.datastax.samples.ExampleUtils.createUdtVideoFormat;

import java.net.InetSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sample code to create tables, types and objects in a keyspace.
 * 
 * Pre-requisites:
 * - Cassandra running locally (127.0.0.1, port 9042)
 * - Keyspace killrvideo created {@link SampleCode4x_CONNECT_CreateKeyspace}
 */
import com.datastax.oss.driver.api.core.CqlSession;


/**
 * Sample code to create tables, types and objects in a keyspace.
 * 
 * Pre-requisites:
 * - Cassandra running locally (127.0.0.1, port 9042)
 */
public class SampleCode4x_CONNECT_CreateSchema implements ExampleSchema {
    
    /** Logger for the class. */
    private static Logger LOGGER = LoggerFactory.getLogger(SampleCode4x_CONNECT_CreateSchema.class);
    
    /** StandAlone (vs JUNIT) to help you running. */
    public static void main(String[] args) {
        LOGGER.info("Starting 'CreateSchema' sample...");
        
        try (CqlSession cqlSession = CqlSession.builder()
                .addContactPoint(new InetSocketAddress("127.0.0.1", 9042))
                .withLocalDatacenter("datacenter1")
                .withKeyspace(KEYSPACE_NAME)
                .build()) {
                createUdtVideoFormat(cqlSession);
                createTableUser(cqlSession);
                createTableVideo(cqlSession);
                createTableVideoViews(cqlSession);
                createTableCommentByVideo(cqlSession);
                createTableCommentByUser(cqlSession);
        }
        LOGGER.info("[OK] Success");
        System.exit(0);
    }
   
}
