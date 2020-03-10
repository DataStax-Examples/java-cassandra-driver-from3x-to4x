package com.datastax.samples;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

/**
 * Sample code to create tables, types and objects in a keyspace.
 * 
 * Pre-requisites:
 * 1. Astra instance started
 * 2. Download the secure connect bundle (instructions for GCP and AWS), that contains connection information such as contact points and certificates.
 * 3. Keyspace killrvideo created {@link SampleCode4x_CONNECT_CreateKeyspace}
 * 
 *
 * Doc : https://docs.datastax.com/en/developer/java-driver/4.5/manual/cloud/
 */
public class SampleCode3x_CONNECT_CloudAstra implements ExampleSchema {
    
    /** Logger for the class. */
    private static Logger LOGGER = LoggerFactory.getLogger(SampleCode3x_CONNECT_CloudAstra.class);
    
    /** StandAlone (vs JUNIT) to help you running. */
    public static void main(String[] args) {
        
        // === #1. Connect using programmatic CqlSession builder ===
        
        try(Cluster cluster = Cluster.builder()
                .withCloudSecureConnectBundle(new File("/Users/cedricklunven/Downloads/secure-connect-killrvideo.zip"))
                .withCredentials("killrvideo", "killrvideo")
                .build() ) {
             Session session = cluster.connect("killrvideo");
             LOGGER.info("[OK] Connected to Keyspace {}", session.getLoggedKeyspace());
        }
        LOGGER.info("[OK] Success");
        System.exit(0);
    }
    
}
