package com.datastax.samples;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;

/**
 * Sample code to create tables, types and objects in a keyspace.
 * 
 * Pre-requisites:
 * - Cassandra running locally (127.0.0.1, port 9042)
 * - Keyspace killrvideo created {@link SampleCode3x_CONNECT_CreateKeyspace}
 * 
 * @author DataStax Developer Advocate Team
 * 
 * Need Help ? Join us on community.datastax.com to ask your questions for free.
 */
public class SampleCode3x_CONNECT_DropKeyspace implements ExampleSchema {
    
    /** Logger for the class. */
    private static Logger LOGGER = LoggerFactory.getLogger(SampleCode3x_CONNECT_DropKeyspace.class);
    
    /** StandAlone (vs JUNIT) to help you running. */
    public static void main(String[] args) {
        LOGGER.info("Starting 'DropKeyspace' sample...");
        
        try(Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build()) {
            LOGGER.info("Connected to Cluster");
            ExampleUtils.dropKeyspace(cluster);
        }
        LOGGER.info("[OK] Success");
        System.exit(0);
    }
}
