package com.datastax.samples;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;


/**
 * Standalone class to log metadata of a running cluster.
 * 
 * We expect you to have a running Cassandra on 127.0.0.1 with default port 9042
 */
public class SampleCode3x_CONNECT_ClusterShowMetaData implements ExampleSchema {
    
    /** Logger for the class. */
    private static Logger LOGGER = LoggerFactory.getLogger(SampleCode3x_CONNECT_ClusterShowMetaData.class);
    
    /** StandAlone (vs JUNIT) to help you running. */
    public static void main(String[] args) {
        
        /**
         * Connecting to the cluster using a single endpoint:
         *  - Note that we don't provide any keyspace informations
         */
        try(Cluster cluster = Cluster.builder()
                                     .addContactPoint("127.0.0.1")  // Single EndPoint is required, 2 is better
                                     .withPort(9042)                // Default port, optional here
                                     .build()) {
            
            // Enforcing meta datas to be retrieved
            cluster.init();
            Metadata metadata = cluster.getMetadata();
            LOGGER.info("Connected to cluster '{}'", metadata.getClusterName());
            
            LOGGER.info("Protocol Version: {}", 
                    cluster.getConfiguration()
                    .getProtocolOptions()
                    .getProtocolVersion());
            
            LOGGER.info("Listing available Nodes:");
            for (Host host : metadata.getAllHosts()) {
                LOGGER.info("+ [{}]: datacenter='{}' and rack='{}'", 
                        host.getListenAddress(),
                        host.getDatacenter(), 
                        host.getRack());
            }
            
            LOGGER.info("Listing available keyspaces:");
            for (KeyspaceMetadata meta : metadata.getKeyspaces()) {
                LOGGER.info("+ [{}] \t with replication={}", meta.getName(), meta.getReplication());
            }
            LOGGER.info("[OK] Success");
        }
        System.exit(0);
    }
}
