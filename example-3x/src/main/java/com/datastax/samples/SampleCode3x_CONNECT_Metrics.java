package com.datastax.samples;

import static com.datastax.samples.ExampleUtils.createTableUser;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import static com.datastax.samples.ExampleUtils.createKeyspace;

/**
 * Create keyspace killrvideo if needed and all tables, user defined types.
 * 
 * Pre-requisites:
 * - Cassandra running locally (127.0.0.1, port 9042)
 * 
 * @author DataStax Developer Advocate Team
 * 
 * Need Help ? Join us on community.datastax.com to ask your questions for free.
 */
public class SampleCode3x_CONNECT_Metrics implements ExampleSchema {
    
    /** Logger for the class. */
    private static Logger LOGGER = LoggerFactory.getLogger(SampleCode3x_CONNECT_Metrics.class);
    
    /** StandAlone (vs JUNIT) to help you running. */
    public static void main(String[] args) throws Exception {
        LOGGER.info("Starting 'Metrics' sample...");
        
        // https://docs.datastax.com/en/developer/java-driver/3.8/manual/metrics/
        
        createKeyspace();
        
        // Metrics is enabled by default but you can disable
        try(Session session = Cluster.builder().addContactPoint("127.0.0.1")
                //.withoutMetrics()       // Disable Metrics
                //.withoutJMXReporting()  // Disable JMX
                .build()
                .connect(KEYSPACE_NAME)) {
            createTableUser(session);
            LOGGER.info("Items in tables 'users' : {}" , 
                    session.execute("select * from users")
                           .getAvailableWithoutFetching());
        }
        MetricRegistry myRegistry = new MetricRegistry();
        
        // Reading metrics values
        try(Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build()) {
            cluster.init();
            myRegistry.registerAll(cluster.getMetrics().getRegistry());
            
            // Exporter as CSV
            File csvDestination = File.createTempFile("metrics-", "");
            CsvReporter csvReporter = CsvReporter.forRegistry(cluster.getMetrics().getRegistry())
                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                    .convertRatesTo(TimeUnit.SECONDS)
                    .build(csvDestination.getParentFile());
            csvReporter.start(1, TimeUnit.SECONDS);
            
            // Work with Session
            try(Session session = cluster.connect(KEYSPACE_NAME)) {
                createTableUser(session);
                LOGGER.info("Items in tables 'users' : {}" , 
                        session.execute("select * from users")
                               .getAvailableWithoutFetching());
            }
            
            // Wait to help generation of all CSV
            Thread.sleep(1000);
            LOGGER.info("Metrics files have been generated here {}", csvDestination.getParentFile().getAbsolutePath());
        }
        LOGGER.info("[OK] Success");
        System.exit(0);
    }
   
}
