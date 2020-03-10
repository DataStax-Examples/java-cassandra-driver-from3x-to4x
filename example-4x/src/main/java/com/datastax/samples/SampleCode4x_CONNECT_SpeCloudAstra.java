package com.datastax.samples;

import java.io.File;
import java.nio.file.Paths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;

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
public class SampleCode4x_CONNECT_SpeCloudAstra implements ExampleSchema {
    
    /** Logger for the class. */
    private static Logger LOGGER = LoggerFactory.getLogger(SampleCode4x_CONNECT_SpeCloudAstra.class);
    
    /** StandAlone (vs JUNIT) to help you running. */
    public static void main(String[] args) {
        
        // === #1. Connect using programmatic CqlSession builder ===
        
        try (CqlSession cqlSession = CqlSession.builder()
                .withCloudSecureConnectBundle(Paths.get("/Users/cedricklunven/Downloads/secure-connect-killrvideo.zip"))
                .withAuthCredentials("killrvideo","killrvideo")
                .withKeyspace("killrvideo")
                .build()) {
            
            // Use session
            LOGGER.info("[OK] Connected to Keyspace {}", cqlSession.getKeyspace().get());
            
        }
        
        // === #2. Using the configuration file ===
        String confFilePath = SampleCode4x_CONNECT_DriverConfigLoader
                .class.getResource("/custom_astra.conf").getFile();
        try (CqlSession cqlSession = CqlSession
                .builder()
                .withConfigLoader(DriverConfigLoader.fromFile(new File(confFilePath)))
                .build()) {
            // Use session
            LOGGER.info("[OK] Connected to Keyspace {}", cqlSession.getKeyspace().get());
        }
        LOGGER.info("[OK] Success");
        System.exit(0);
    }
    
}
