package com.datastax.samples;

import java.io.File;
import java.nio.file.Paths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;

/**
 * This class shows how to connect to the DataStax Cloud Cassandra As a Service: ASTRA
 * 
 *  Pre-requisites:
 * ===================
 * 
 * 1. You need an ASTRA intance : go to astra.datastax.com and create an instance there. There is a free tier
 * for you to have a 3-node clusters availables forever. You can find more info on:
 *
 * 2. You need to provide you ASTRA credentials username, password, keyspace 
 * but also the secure bundle ZIP. To download it please follow the instruction on : 
 * https://docs.datastax.com/en/developer/java-driver/4.5/manual/cloud/
 * 
 * 3. You need a java driver version 3.8
 */
public class SampleCode4x_CONNECT_ServiceCloudAstra implements ExampleSchema {
    
    /** Logger for the class. */
    private static Logger LOGGER = LoggerFactory.getLogger(SampleCode4x_CONNECT_ServiceCloudAstra.class);
    
    /** StandAlone (vs JUNIT) to help you running. */
    public static void main(String[] args) {
        
        // ----
        // #1. Connecting explicitely using the CqlSessionBuilder
        // ----
        
        // Those are mandatory to connect to ASTRA
        final String ASTRA_ZIP_FILE = "/Users/cedricklunven/Downloads/secure-connect-killrvideo.zip";
        final String ASTRA_USERNAME = "killrvideo";
        final String ASTRA_PASSWORD = "killrvideo";
        final String ASTRA_KEYSPACE = "killrvideo";
        
        // Check the cloud zip file
        File cloudSecureConnectBundleFile = new File(ASTRA_ZIP_FILE);
        if (!cloudSecureConnectBundleFile.exists()) {
            throw new IllegalStateException("File '" + ASTRA_ZIP_FILE + "' has not been found\n"
                    + "To run this sample you need to download the secure bundle file from ASTRA WebPage\n"
                    + "More info here:");
        }
        
        // Connect
        try (CqlSession cqlSession = CqlSession.builder()
                .withCloudSecureConnectBundle(Paths.get(ASTRA_ZIP_FILE))
                .withAuthCredentials(ASTRA_USERNAME, ASTRA_PASSWORD)
                .withKeyspace(ASTRA_KEYSPACE)
                .build()) {
            LOGGER.info("[OK] Welcome to ASTRA. Connected to Keyspace {}", cqlSession.getKeyspace().get());
        }
        
        // ----
        // #2. Delegating configuration parameters to dedicated file
        // ----
        
        /*
         * In this sample target conf is located in src/main/resources, 
         * this is what it looks like
         * 
         * datastax-java-driver {
         *   basic {
         *     session-keyspace = killrvideo
         *     cloud {
         *       secure-connect-bundle = /Users/cedricklunven/Downloads/secure-connect-killrvideo.zip
         *     }
         *   }
         *   advanced {
         *     auth-provider {
         *       class = PlainTextAuthProvider
         *       username = killrvideo
         *       password = killrvideo
         *     }
         *   }
         * }
         */
        String confFilePath = SampleCode4x_CONNECT_DriverConfigLoader
                .class.getResource("/custom_astra.conf").getFile();
        
        try (CqlSession cqlSession = CqlSession.builder()
                .withConfigLoader(DriverConfigLoader.fromFile(new File(confFilePath))).build()) {
            // Use session
            LOGGER.info("[OK] Welcome to ASTRA (conf file). Connected to Keyspace {}", 
                    cqlSession.getKeyspace().get());
        }
        
        LOGGER.info("[OK] Success");
        System.exit(0);
    }
    
}
