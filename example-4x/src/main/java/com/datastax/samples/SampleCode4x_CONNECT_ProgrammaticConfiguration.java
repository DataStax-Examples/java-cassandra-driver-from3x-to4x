package com.datastax.samples;

import java.time.Duration;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;

/**
 * Sample code to create tables, types and objects in a keyspace.
 * 
 * Pre-requisites:
 * - Cassandra running locally (127.0.0.1, port 9042)
 * - Keyspace killrvideo created {@link SampleCode4x_CONNECT_CreateKeyspace}
 * 
 * Doc : https://docs.datastax.com/en/developer/java-driver/4.5/manual/core/configuration/
 */
public class SampleCode4x_CONNECT_ProgrammaticConfiguration implements ExampleSchema {
    
    /** Logger for the class. */
    private static Logger LOGGER = LoggerFactory.getLogger(SampleCode4x_CONNECT_ProgrammaticConfiguration.class);
    
    /** StandAlone (vs JUNIT) to help you running. */
    public static void main(String[] args) {
        
        DriverConfigLoader loader = DriverConfigLoader.programmaticBuilder()
            .withStringList(DefaultDriverOption.CONTACT_POINTS, Arrays.asList("127.0.0.1:9042"))
            .withString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, "datacenter1")
            .withString(DefaultDriverOption.SESSION_KEYSPACE, KEYSPACE_NAME)
            .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(5))
            
            // If you want to override with an execution profile
            .startProfile("slow")
            .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30))
            .endProfile()
            .build();
       
        
        // Use it to create the session
        try (CqlSession cqlSession = CqlSession.builder().withConfigLoader(loader).build()) {
            
            // Use session
            LOGGER.info("[OK] Connected to Keyspace {}", cqlSession.getKeyspace().get());
        }
        LOGGER.info("[OK] Success");
        System.exit(0);
    }
    
}
