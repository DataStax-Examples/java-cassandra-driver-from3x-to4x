package com.datastax.samples;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

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
 * https://docs.datastax.com/en/developer/java-driver/3.8/manual/cloud/
 * 
 * 3. You need a java driver version 3.8
 * 
 * @author DataStax Developer Advocate Team
 * 
 * Need Help ? Join us on community.datastax.com to ask your questions for free.
 */
public class SampleCode3x_CONNECT_ServiceCloudAstra implements ExampleSchema {
    
    /** Logger for the class. */
    private static Logger LOGGER = LoggerFactory.getLogger(SampleCode3x_CONNECT_ServiceCloudAstra.class);
    
    /** StandAlone (vs JUNIT) to help you running. */
    public static void main(String[] args) {
        LOGGER.info("Starting 'ServiceCloudAstra' sample...");
        
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
        try(Cluster cluster = Cluster.builder()
                .withCloudSecureConnectBundle(cloudSecureConnectBundleFile)
                .withCredentials(ASTRA_USERNAME, ASTRA_PASSWORD)
                .build() ) {
             Session session = cluster.connect(ASTRA_KEYSPACE);
             LOGGER.info("[OK] Welcome to ASTRA. Connected to Keyspace {}", session.getLoggedKeyspace());
        }
        
        LOGGER.info("[OK] Success");
        System.exit(0);
    }
    
}
