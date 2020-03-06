package com.datastax.samples;

import java.net.InetSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;

/**
 * Standalone class to log metadata of a running cluster.
 * 
 * We expect you to have a running Cassandra on 127.0.0.1 with default port 9042
 */
public class SampleCode4x_CONNECT_ClusterShowMetaData {
    
    /** Logger for the class. */
    private static Logger LOGGER = LoggerFactory.getLogger(SampleCode4x_CONNECT_ClusterShowMetaData.class);
    
    /** StandAlone (vs JUNIT) to help you running. */
    public static void main(String[] args) {
        
        try (CqlSession cqlSession = CqlSession.builder()
                .addContactPoint(new InetSocketAddress("127.0.0.1", 9042))
                .withLocalDatacenter("datacenter1")
                .build()) {
            
            LOGGER.info("Connected to cluster with Session '{}'",
                    cqlSession.getName());
            
            LOGGER.info("Protocol Version: {}", 
                    cqlSession.getContext().getProtocolVersion());
            
            Metadata metaData = cqlSession.getMetadata();
            
            LOGGER.info("Listing available Nodes:");
            for (Node host : metaData.getNodes().values()) {
                LOGGER.info("+ [{}]: datacenter='{}' and rack='{}'", 
                        host.getListenAddress().orElse(null),
                        host.getDatacenter(), 
                        host.getRack());
            }
            
            LOGGER.info("Listing available keyspaces:");
            for (KeyspaceMetadata meta : metaData.getKeyspaces().values()) {
                LOGGER.info("+ [{}] \t with replication={}", meta.getName(), meta.getReplication());
            }
            
            LOGGER.info("[OK] Success");
        }
        System.exit(0);
    }
}
