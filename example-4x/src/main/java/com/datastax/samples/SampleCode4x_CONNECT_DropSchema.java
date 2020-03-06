package com.datastax.samples;

import static com.datastax.samples.ExampleUtils.dropTableIfExists;
import static com.datastax.samples.ExampleUtils.dropTypeIffExists;

import java.net.InetSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.CqlSession;

/**
 * Sample code to create tables, types and objects in a keyspace.
 * 
 * Pre-requisites:
 * - Cassandra running locally (127.0.0.1, port 9042)
 * - Keyspace killrvideo created {@link SampleCode4x_CONNECT_CreateKeyspace}
 * 
 * @author Cedrick LUNVEN (@clunven)
 */
public class SampleCode4x_CONNECT_DropSchema implements ExampleSchema {
    
    /** Logger for the class. */
    private static Logger LOGGER = LoggerFactory.getLogger(SampleCode4x_CONNECT_DropSchema.class);
    
    /** StandAlone (vs JUNIT) to help you running. */
    public static void main(String[] args) {
        
        try (CqlSession cqlSession = CqlSession.builder()
                .addContactPoint(new InetSocketAddress("127.0.0.1", 9042))
                .withLocalDatacenter("datacenter1")
                .withKeyspace(KEYSPACE_NAME)
                .build()) {
            LOGGER.info("[OK] Connected to Keyspace");
            dropTableIfExists(cqlSession, COMMENT_BY_VIDEO_TABLENAME);
            dropTableIfExists(cqlSession, COMMENT_BY_USER_TABLENAME);
            dropTableIfExists(cqlSession, VIDEO_VIEWS_TABLENAME);
            dropTableIfExists(cqlSession, VIDEO_TABLENAME);
            dropTableIfExists(cqlSession, USER_TABLENAME);
            dropTypeIffExists(cqlSession, UDT_VIDEO_FORMAT_NAME);
        }
        LOGGER.info("[OK] Success");
        System.exit(0);
    }
    
}
