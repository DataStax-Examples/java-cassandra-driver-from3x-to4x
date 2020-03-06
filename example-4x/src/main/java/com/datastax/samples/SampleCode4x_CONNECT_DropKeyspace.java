package com.datastax.samples;

import java.net.InetSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;

/**
 * Sample code to create tables, types and objects in a keyspace.
 * 
 * Pre-requisites:
 * - Cassandra running locally (127.0.0.1, port 9042)
 * - Keyspace killrvideo created {@link SampleCode4x_CONNECT_CreateKeyspace}
 * 
 * @author Cedrick LUNVEN (@clunven)
 */
public class SampleCode4x_CONNECT_DropKeyspace implements ExampleSchema {
    
    /** Logger for the class. */
    private static Logger LOGGER = LoggerFactory.getLogger(SampleCode4x_CONNECT_DropKeyspace.class);
    
    /** StandAlone (vs JUNIT) to help you running. */
    public static void main(String[] args) {
        try (CqlSession cqlSession = CqlSession.builder()
                .addContactPoint(new InetSocketAddress("127.0.0.1", 9042))
                .withLocalDatacenter("datacenter1")
                .build()) {
            cqlSession.execute(SchemaBuilder.dropKeyspace(KEYSPACE_NAME).ifExists().build());
            LOGGER.info("+ Keyspace '{}' has been dropped (if needed).", KEYSPACE_NAME);
        }
        LOGGER.info("[OK] Success");
        System.exit(0);
    }
}
